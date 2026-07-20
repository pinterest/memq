#!/bin/bash
# End-to-end test: Java producer -> MemQ broker (fs storage) -> Kafka -> Rust consumer
#
# Usage: ./run-e2e.sh
#
# Prerequisites: Maven (or mvnw), Java, Kafka reachable on localhost:9092
#
# This script:
#   1. Detects or starts Kafka (Docker if needed)
#   2. Builds MemQ broker if not already built
#   3. Starts the broker
#   4. Produces 10 messages via Java producer
#   5. Runs Rust consumer to read all 10 messages

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../" && pwd)"
KAFKA_COMPOSE="$SCRIPT_DIR/tests/docker-compose.yml"
STORAGE_DIR="/tmp/memq-storage"
BROKER_CONFIG="$ROOT_DIR/deploy/configs/test-fs.yaml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[OK]${NC}  $*"; }
error() { echo -e "${RED}[FAIL]${NC} $*"; }
fail()  { error "$1"; exit "${2:-1}"; }
step()  { echo -e "${YELLOW}>>> $*${NC}"; }

MAVEN_CMD="mvn"
if [ -f "$ROOT_DIR/mvnw" ]; then
    MAVEN_CMD="./mvnw"
fi
MVN_OPTS="-DskipTests -Dmaven.javadoc.skip=true -q"

# --- Check prerequisites ---
step "Checking prerequisites..."

if ! command -v java &>/dev/null; then
    fail "java not found. Install JDK 11+ and add to PATH."
fi
info "java $(java -version 2>&1 | head -1)"

if ! command -v mvn &>/dev/null; then
    fail "Maven not found. Install Maven and add to PATH."
fi
info "Maven $(mvn -version 2>&1 | head -1)"

if ! command -v cargo &>/dev/null; then
    export PATH="$HOME/.cargo/bin:$PATH"
fi
if ! command -v cargo &>/dev/null; then
    fail "cargo (Rust) not found. Install Rust and add ~/.cargo/bin to PATH."
fi
info "Rust $(cargo --version 2>&1)"

# --- Step 1: Detect Kafka ---
USED_DOCKER="false"

function kafka_is_up() {
    nc -z localhost 9092 2>/dev/null
}

if kafka_is_up; then
    step "Kafka detected on localhost:9092 (host), skipping Docker..."
else
    step "Kafka not detected, starting Docker Kafka..."
    if ! command -v docker-compose &>/dev/null; then
        fail "docker-compose not found. Install Docker Compose or start a Kafka broker on localhost:9092."
    fi
    docker-compose -f "$KAFKA_COMPOSE" pull > /dev/null 2>&1 || true
    docker-compose -f "$KAFKA_COMPOSE" up -d > /dev/null 2>&1
    USED_DOCKER="true"

    for i in $(seq 1 30); do
        if kafka_is_up; then
            info "Kafka is ready"
            break
        fi
        if [ "$i" -eq 30 ]; then
            fail "Kafka did not become ready in 30s"
        fi
        sleep 1
    done
fi

# --- Step 2: Create notification topic ---
step "Creating Kafka topic: TestTopicNotifications (3 partitions)..."

if [ "$USED_DOCKER" = "true" ]; then
    docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \
      --topic TestTopicNotifications --partitions 3 --replication-factor 1 --create \
      --if-not-exists > /dev/null 2>&1 || true
else
    for tpath in /opt/kafka/bin/kafka-topics.sh /usr/local/kafka/bin/kafka-topics.sh ~/kafka/bin/kafka-topics.sh; do
        if [ -f "$tpath" ]; then
            "$tpath" --bootstrap-server localhost:9092 \
              --topic TestTopicNotifications --partitions 3 --replication-factor 1 --create \
              --if-not-exists > /dev/null 2>&1 || true
            break
        fi
    done
fi
info "Topic ready"

# --- Step 3: Build & start MemQ broker ---
step "Cleaning previous storage..."
rm -rf "$STORAGE_DIR"

BROKER_JAR="$ROOT_DIR/memq/target/memq-*.jar"
DEP_DIR="$ROOT_DIR/memq/target/dependency"

# Always rebuild deps to ensure clean SLF4J state
rm -rf "$DEP_DIR"

if ! ls $BROKER_JAR 2>/dev/null | head -1 | grep -q .; then
    step "Building MemQ broker (this may take a few minutes)..."
    cd "$ROOT_DIR"
    $MAVEN_CMD package $MVN_OPTS > /tmp/memq-build.log 2>&1 || {
        error "Build failed. See /tmp/memq-build.log"
        tail -20 /tmp/memq-build.log
        exit 1
    }
fi

# Copy dependencies for classpath, then remove conflicting SLF4J impls for Dropwizard
$MAVEN_CMD dependency:copy-dependencies -DoutputDirectory="$DEP_DIR" $MVN_OPTS > /dev/null 2>&1
rm -f "$DEP_DIR/slf4j-simple"* "$DEP_DIR/slf4j-log4j12"* 2>/dev/null || true
info "Broker ready"

BROKER_JAR="$(ls $BROKER_JAR 2>/dev/null | head -1)"
if [ -z "$BROKER_JAR" ]; then
    fail "No broker jar found in memq/target/"
fi
info "Broker jar: $BROKER_JAR"

# Kill existing broker on port 8080
EXISTING_PID=$(lsof -ti:8080 2>/dev/null || true)
if [ -n "$EXISTING_PID" ]; then
    step "Killing existing broker (PID: $EXISTING_PID)..."
    kill -9 $EXISTING_PID 2>/dev/null || true
    sleep 2
fi

step "Starting MemQ broker..."
BROKER_CP="$BROKER_JAR:$DEP_DIR/*"
java -cp "$BROKER_CP" com.pinterest.memq.core.MemqMain server "$BROKER_CONFIG" > /tmp/memq-broker.log 2>&1 &
BROKER_PID=$!
info "Broker started (PID: $BROKER_PID)"

# Wait for broker HTTP endpoint
for i in $(seq 1 60); do
    if curl -s http://localhost:8080 > /dev/null 2>&1; then
        info "Broker is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        error "Broker did not become ready in 60s"
        cat /tmp/memq-broker.log
        fail ""
    fi
    sleep 1
done

# --- Step 4: Produce 10 messages via Java producer ---
step "Producing 10 messages via Java producer..."
cd "$ROOT_DIR"

# Find Java client dependencies
CLIENT_JAR="$(ls memq-client/target/memq-client-*.jar 2>/dev/null | head -1)"
COMMONS_JAR="$(ls memq-commons/target/memq-commons-*.jar 2>/dev/null | head -1)"
CLIENT_ALL_JAR="$(ls memq-client-all/target/memq-client-all-*.jar 2>/dev/null | head -1)"

if [ -n "$CLIENT_ALL_JAR" ]; then
    # Use fat jar - has all dependencies bundled
    CLASSPATH="$CLIENT_ALL_JAR"
elif [ -n "$CLIENT_JAR" ] && [ -n "$COMMONS_JAR" ]; then
    CLASSPATH="$CLIENT_JAR:$COMMONS_JAR"
else
    step "Building Java client..."
    $MAVEN_CMD package -pl memq-client,memq-commons $MVN_OPTS > /tmp/memq-client-build.log 2>&1 || {
        error "Client build failed. See /tmp/memq-client-build.log"
        tail -20 /tmp/memq-client-build.log
        fail ""
    }
    CLIENT_JAR="$(ls memq-client/target/memq-client-*.jar 2>/dev/null | head -1)"
    COMMONS_JAR="$(ls memq-commons/target/memq-commons-*.jar 2>/dev/null | head -1)"
    CLASSPATH="$CLIENT_JAR:$COMMONS_JAR"
fi

# Create serverset for Java producer
rm -rf /tmp/memq_serverset
mkdir -p /tmp/memq_serverset
cat > /tmp/memq_serverset/serverset <<'EOF'
{"az": "us-east-1a", "ip": "127.0.0.1", "port": "8080", "stage_name": "prototype", "version": "none", "weight": 1}
EOF

# Minimal 10-message producer
cat > /tmp/E2EProducer.java << 'JAVAEOF'
import com.pinterest.memq.client.commons.serde.ByteArraySerializer;
import com.pinterest.memq.client.producer2.MemqProducer;
public class E2EProducer {
    public static void main(String[] args) throws Exception {
        MemqProducer<byte[], byte[]> producer = new MemqProducer.Builder<byte[], byte[]>()
            .disableAcks(false)
            .keySerializer(new ByteArraySerializer())
            .valueSerializer(new ByteArraySerializer())
            .topic("test")
            .cluster("local")
            .bootstrapServers("127.0.0.1:9093")
            .build();

        for (int i = 0; i < 10; i++) {
            final int idx = i;
            byte[] key = ("e2e-key-" + idx).getBytes("UTF-8");
            byte[] value = ("e2e-value-" + idx).getBytes("UTF-8");
            producer.write(key, value, System.nanoTime()).get();
            System.out.println("  Message " + (idx + 1) + "/10 written");
        }
        producer.flush();
        producer.close();
        System.out.println("All 10 messages produced successfully");
    }
}
JAVAEOF

javac -cp "$CLASSPATH" /tmp/E2EProducer.java 2>&1 || {
    error "Failed to compile E2EProducer. Classpath: $CLASSPATH"
    fail ""
}
java -cp "$CLASSPATH:/tmp" E2EProducer 2>&1

# --- Step 5: Run Rust consumer ---
step "Running Rust consumer to read messages..."
cd "$SCRIPT_DIR"
export PATH="$HOME/.cargo/bin:$PATH"
cargo run --example e2e_broker

echo ""
info "E2E pipeline completed successfully!"
info "Java producer -> MemQ broker (fs storage) -> Kafka -> Rust consumer: ALL PASSED"
