FROM rust:1.95-slim AS builder

ENV APP_GROUP=appgroup \
    APP_GID=1000 \
    APP_USER=appuser \
    APP_UID=1000

# Cache build dependencies
WORKDIR /build
COPY Cargo.toml ./
COPY Cargo.lock ./
RUN mkdir -p src
RUN echo 'fn main() {}' > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -f src/main.rs

# Build the application
COPY ./src ./src
RUN cargo build --release

# Create Group and User
RUN groupadd -g $APP_GID $APP_GROUP && useradd -l -u $APP_UID -g $APP_GROUP -s /sbin/nologin $APP_USER
RUN chown $APP_UID:$APP_GID /build/target/release/port-cycle

FROM gcr.io/distroless/cc-debian13:latest AS runtime

# Copy builder files
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/target/release/port-cycle /app/port-cycle

# Start the application
WORKDIR /app
USER $APP_UID
ENTRYPOINT ["/app/port-cycle"]
