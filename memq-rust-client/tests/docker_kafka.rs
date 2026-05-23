/// Docker-based Kafka test fixture for integration tests.
///
/// Spins up Zookeeper + Kafka via docker-compose, waits for readiness,
/// creates the notification topic, provides a shutdown hook.

use std::process::Command;
use std::time::{Duration, Instant};
use std::sync::OnceLock;

const COMPOSE_FILE: &str = "tests/docker-compose.yml";
const KAFKA_BOOTSTRAP: &str = "localhost:9092";
const KAFKA_PORT: u16 = 9092;
const READINESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Module-level flag so Kafka is only started once per test session.
static STARTED: OnceLock<bool> = OnceLock::new();

/// Starts the Docker Kafka infrastructure and waits for it to be ready.
/// Returns `true` if Kafka is reachable, `false` otherwise.
///
/// Only starts once per test session (cached via `OnceLock`).
pub fn start() -> bool {
    // Return cached result
    if let Some(&already) = STARTED.get() {
        return already;
    }

    // Check if already running (e.g., from a previous test run)
    if is_ready() {
        STARTED.set(true).ok();
        return true;
    }

    // Pull images
    println!("  [docker_kafka] Starting Kafka via docker-compose (pulling images)...");
    let status = Command::new("docker-compose")
        .arg("-f")
        .arg(COMPOSE_FILE)
        .arg("pull")
        .status()
        .expect("Failed to run docker-compose pull");
    if !status.success() {
        eprintln!("  [docker_kafka] Failed to pull docker-compose images");
        STARTED.set(false).ok();
        return false;
    }

    // Start services
    let status = Command::new("docker-compose")
        .arg("-f")
        .arg(COMPOSE_FILE)
        .arg("up")
        .arg("-d")
        .status()
        .expect("Failed to run docker-compose up");
    if !status.success() {
        eprintln!("  [docker_kafka] Failed to start docker-compose services");
        STARTED.set(false).ok();
        return false;
    }

    // Wait for Kafka to be ready
    println!("  [docker_kafka] Waiting for Kafka to become ready...");
    let deadline = Instant::now() + READINESS_TIMEOUT;
    while Instant::now() < deadline {
        if is_ready() {
            println!("  [docker_kafka] Kafka is ready at {}", KAFKA_BOOTSTRAP);
            STARTED.set(true).ok();
            return true;
        }
        std::thread::sleep(Duration::from_secs(3));
    }

    eprintln!("  [docker_kafka] Kafka did not become ready within {}s", READINESS_TIMEOUT.as_secs());
    // Dump logs for debugging
    let _ = Command::new("docker-compose")
        .arg("-f")
        .arg(COMPOSE_FILE)
        .arg("logs")
        .status();
    STARTED.set(false).ok();
    false
}

/// Stops the Docker Kafka infrastructure.
pub fn stop() {
    let _ = Command::new("docker-compose")
        .arg("-f")
        .arg(COMPOSE_FILE)
        .arg("down")
        .status();
}

/// Checks if Kafka is reachable on localhost:9092.
fn is_ready() -> bool {
    use std::net::TcpStream;
    TcpStream::connect(KAFKA_BOOTSTRAP).is_ok()
        && TcpStream::connect("localhost:2181").is_ok()
}

/// Creates a topic on the running Kafka broker.
pub fn create_topic(topic: &str, partitions: i32) -> bool {
    let status = Command::new("docker")
        .args([
            "exec", "kafka", "kafka-topics.sh",
            "--bootstrap-server", KAFKA_BOOTSTRAP,
            "--create", "--topic", topic,
            "--partitions", &partitions.to_string(),
            "--replication-factor", "1",
            "--if-not-exists",
        ])
        .status();
    status.map(|s| s.success()).unwrap_or(false)
}
