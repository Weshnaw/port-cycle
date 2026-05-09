use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{env, sync::Arc};

use k8s_openapi::api::core::v1::Node;
use kube::Api;
use kube::api::ListParams;
use kube_leader_election::{LeaseLock, LeaseLockParams, LeaseLockResult};
use reqwest::Client;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{interval, timeout};

struct Config {
    controller: String,
    console: String,
    site: String,
    device: String,
    key: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let require =
            |key: &str| env::var(key).map_err(|_| format!("Missing required env var: {key}"));

        Ok(Self {
            controller: require("UNIFI_CONTROLLER")?,
            console: require("UNIFI_CONSOLE")?,
            site: require("UNIFI_SITE")?,
            device: require("UNIFI_DEVICE")?,
            key: require("UNIFI_KEY")?,
        })
    }
}

fn build_client() -> Result<Client, reqwest::Error> {
    Client::builder().timeout(Duration::from_secs(15)).build()
}

async fn power_cycle(client: &Client, cfg: &Config, port: &u32) -> Result<(), Box<dyn Error>> {
    println!("Power cycling port: {}...", port);

    let url = format!(
        "{}/connector/consoles/{}/proxy/network/integration/v1/sites/{}/devices/{}/interfaces/ports/{}/actions",
        cfg.controller, cfg.console, cfg.site, cfg.device, port
    );

    let res = client
        .post(&url)
        .json(&json!({
            "action": "POWER_CYCLE"
        }))
        .header("X-API-Key", &cfg.key)
        .send()
        .await?;

    if !res.status().is_success() {
        let status = res.status();
        let body = res.text().await.unwrap_or_default();
        return Err(format!("Failed to power cylce port: {status} — {body}").into());
    }

    println!("Power cycle completed for port {}.", port);
    Ok(())
}

const RECOVERY_THRESHOLD: u32 = 5;
const FAILURE_THRESHOLD: u32 = 3;
struct NodeState {
    consecutive_failures: u32,
    consecutive_successes: u32,
    remediating: bool,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let cfg = Config::from_env().unwrap_or_else(|e| {
        eprintln!("Configuration error: {e}\n");
        eprintln!("Required environment variables:");
        eprintln!("  UNIFI_CONTROLLER  — e.g. https://api.ui.com/v1");
        eprintln!("  UNIFI_CONSOLE     — Unifi Console ID");
        eprintln!("  UNIFI_SITE        — Unifi Site ID");
        eprintln!("  UNIFI_DEVICE      — Unifi Device ID");
        eprintln!("  UNIFI_KEY         — Unifi API Key");
        std::process::exit(1);
    });
    let cycle_client = build_client().unwrap_or_else(|e| {
        eprintln!("Failed to build HTTP client: {e}");
        std::process::exit(1);
    });

    let client = kube::Client::try_default().await?;
    let pod_name = std::env::var("POD_NAME").unwrap_or("port-cycle".to_string());

    let leadership = LeaseLock::new(
        client.clone(),
        "bfall-me",
        LeaseLockParams {
            holder_id: pod_name,
            lease_name: "node-checker-lock".into(),
            lease_ttl: Duration::from_secs(15),
        },
    );

    let is_leader = Arc::new(AtomicBool::new(false));
    let is_leader_checker = is_leader.clone();

    // Renew lease every 5s in background
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;
            match leadership.try_acquire_or_renew().await {
                Ok(LeaseLockResult::Acquired(_)) => is_leader.store(true, Ordering::Relaxed),
                Ok(LeaseLockResult::NotAcquired(_)) => is_leader.store(false, Ordering::Relaxed),
                Err(e) => {
                    eprintln!("Lease error: {e}");
                    is_leader.store(false, Ordering::Relaxed);
                }
            }
        }
    });

    // Main check loop
    let nodes: Api<Node> = Api::all(client);
    let mut ticker = interval(Duration::from_secs(10));

    let map: HashMap<String, u32> = [
        ("n100-12gb-01".to_string(), 19),
        ("n100-12gb-02".to_string(), 20),
        ("n100-12gb-03".to_string(), 18),
    ]
    .into_iter()
    .collect();

    let nodes_state: Arc<Mutex<HashMap<String, NodeState>>> = Arc::new(Mutex::new(HashMap::new()));
    println!("Starting loop");
    loop {
        ticker.tick().await;

        if !is_leader_checker.load(Ordering::Relaxed) {
            continue; // standby — do nothing
        }

        let node_list = nodes.list(&ListParams::default()).await?;
        for node in node_list.items {
            let name = node.metadata.name.unwrap_or_default();
            let ip = node
                .status
                .as_ref()
                .and_then(|s| s.addresses.as_ref())
                .and_then(|addrs| addrs.iter().find(|a| a.type_ == "InternalIP"))
                .map(|a| a.address.clone())
                .unwrap_or_default();

            if let Some(port) = map.get(&name)
                && !ip.is_empty()
            {
                println!("Checking node: {}:{}", &name, &ip);
                let reachable = is_node_reachable(&ip, 10250).await;

                let mut states = nodes_state.lock().await;
                let state = states.entry(name.clone()).or_insert(NodeState {
                    consecutive_failures: 0,
                    consecutive_successes: 0,
                    remediating: false,
                });

                if reachable {
                    state.consecutive_failures = 0;
                    if state.remediating {
                        state.consecutive_successes += 1;
                        println!(
                            "Node {name} reachable, waiting for stability ({}/{RECOVERY_THRESHOLD})",
                            state.consecutive_successes
                        );

                        if state.consecutive_successes >= RECOVERY_THRESHOLD {
                            println!("Node {name} is stable...");
                            state.remediating = false;
                        }
                    }
                } else {
                    state.consecutive_successes = 0;
                    if state.remediating {
                        println!("Waiting for {name} to become reachable again...");
                    } else {
                        state.consecutive_failures += 1;

                        println!(
                            "Node {name} unreachable, waiting for failure threshold ({}/{FAILURE_THRESHOLD})",
                            state.consecutive_failures
                        );

                        if state.consecutive_failures >= FAILURE_THRESHOLD {
                            println!("Node {name} is unreachable — triggering power cycle");
                            state.remediating = true;
                            drop(states);
                            trigger_event(&name, &cycle_client, &cfg, port).await;
                        }
                    }
                }
            }
        }
    }
}

async fn is_node_reachable(ip: &str, port: u16) -> bool {
    let addr = format!("{ip}:{port}");
    timeout(Duration::from_secs(2), TcpStream::connect(&addr))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false)
}

async fn trigger_event(node_name: &str, client: &Client, cfg: &Config, port: &u32) {
    println!("Remediating node: {node_name}");
    power_cycle(client, cfg, port)
        .await
        .expect("Failed to cycle poe");
}
