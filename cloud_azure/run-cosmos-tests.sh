#!/bin/bash
# Script to run Cosmos DB tests with Cosmos Emulator in Docker

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
EMULATOR_CONTAINER_NAME="cosmos-emulator-test"
EMULATOR_IMAGE="mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"
STARTUP_TIMEOUT=180  # seconds to wait for emulator to be ready
CERT_FILE="/tmp/cosmos_emulator.cert"

echo -e "${GREEN}=== Cosmos DB Test Runner ===${NC}"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker not found. Please install Docker to run Cosmos DB emulator.${NC}"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker daemon is not running. Please start Docker.${NC}"
    exit 1
fi

# Function to check if emulator is ready
check_emulator_ready() {
    # Use the proper readiness probe endpoint
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/ready 2>/dev/null || echo "000")
    echo "$HTTP_CODE"
}

# Function to cleanup
cleanup() {
    if [ "$KEEP_EMULATOR" != "true" ]; then
        echo -e "${YELLOW}Stopping Cosmos emulator...${NC}"
        docker stop $EMULATOR_CONTAINER_NAME 2>/dev/null || true
        docker rm $EMULATOR_CONTAINER_NAME 2>/dev/null || true
    else
        echo -e "${YELLOW}Keeping emulator running (KEEP_EMULATOR=true)${NC}"
        echo -e "${YELLOW}To stop manually: docker stop $EMULATOR_CONTAINER_NAME${NC}"
    fi
}

# Trap to cleanup on exit
trap cleanup EXIT

# Check if emulator is already running
if docker ps | grep -q $EMULATOR_CONTAINER_NAME; then
    echo -e "${YELLOW}Cosmos emulator already running, using existing instance...${NC}"
else
    # Stop and remove any existing stopped container
    docker stop $EMULATOR_CONTAINER_NAME 2>/dev/null || true
    docker rm $EMULATOR_CONTAINER_NAME 2>/dev/null || true

    echo -e "${YELLOW}Starting Cosmos DB emulator...${NC}"
    echo -e "${YELLOW}This may take 1-2 minutes on first run (downloading image)...${NC}"

    # Start the emulator with correct port mappings
    # Port 8080: Health check endpoints
    # Port 8081: HTTPS management endpoint
    # Port 1234: Data plane endpoint
    docker run --detach \
        --name $EMULATOR_CONTAINER_NAME \
        --publish 8080:8080 \
        --publish 8081:8081 \
        --publish 1234:1234 \
        $EMULATOR_IMAGE \
        --protocol https

    # Wait for emulator to be ready
    echo -e "${YELLOW}Waiting for emulator to be ready (max ${STARTUP_TIMEOUT}s)...${NC}"
    ELAPSED=0
    while [ $ELAPSED -lt $STARTUP_TIMEOUT ]; do
        HTTP_CODE=$(check_emulator_ready)
        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "302" ]; then
            echo -e "${GREEN}Emulator is ready!${NC}"
            break
        fi

        if [ $((ELAPSED % 10)) -eq 0 ]; then
            echo -e "${YELLOW}Still waiting... (${ELAPSED}s elapsed)${NC}"
        fi

        sleep 2
        ELAPSED=$((ELAPSED + 2))
    done

    # Final check
    HTTP_CODE=$(check_emulator_ready)
    if [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "302" ]; then
        echo -e "${RED}Error: Emulator failed to start within ${STARTUP_TIMEOUT}s${NC}"
        echo -e "${YELLOW}Check logs with: docker logs $EMULATOR_CONTAINER_NAME${NC}"
        exit 1
    fi

    # Give it a few more seconds to fully initialize
    echo -e "${YELLOW}Emulator started, waiting 5 more seconds for full initialization...${NC}"
    sleep 5
fi

# Show emulator info
echo -e "${GREEN}Emulator is ready!${NC}"
echo -e "${GREEN}  Health endpoint: http://localhost:8080/ready${NC}"
echo -e "${GREEN}  Data Explorer: https://localhost:8081/_explorer/index.html${NC}"
echo -e "${GREEN}  Data plane: http://localhost:1234${NC}"

# Extract and trust the emulator's self-signed certificate
echo -e "${YELLOW}Extracting emulator certificate...${NC}"

# Extract certificate using openssl
openssl s_client -connect localhost:8081 </dev/null 2>/dev/null | \
    sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > "$CERT_FILE"

if [ ! -s "$CERT_FILE" ]; then
    echo -e "${RED}Error: Failed to extract certificate${NC}"
    exit 1
fi

echo -e "${GREEN}Certificate extracted to: $CERT_FILE${NC}"

# Import certificate into Java truststore
echo -e "${YELLOW}Setting up Java truststore with emulator certificate...${NC}"

# Create a temporary truststore that includes the emulator certificate
TRUSTSTORE_PATH="/tmp/cosmos-truststore.jks"
TRUSTSTORE_PASSWORD="changeit"

# Remove existing truststore if it exists
rm -f "$TRUSTSTORE_PATH"

# Find the system cacerts file
JAVA_CACERTS=""
if [ -n "$JAVA_HOME" ]; then
    JAVA_CACERTS="$JAVA_HOME/lib/security/cacerts"
elif command -v java >/dev/null 2>&1; then
    # Try to find java home from the java command
    JAVA_BIN=$(which java)
    if [ -L "$JAVA_BIN" ]; then
        JAVA_BIN=$(readlink -f "$JAVA_BIN" 2>/dev/null || readlink "$JAVA_BIN")
    fi
    POSSIBLE_JAVA_HOME=$(dirname $(dirname "$JAVA_BIN"))
    JAVA_CACERTS="$POSSIBLE_JAVA_HOME/lib/security/cacerts"
fi

# Copy system cacerts to our custom truststore if found
if [ -f "$JAVA_CACERTS" ]; then
    echo -e "${YELLOW}Copying system CA certificates from $JAVA_CACERTS...${NC}"
    cp "$JAVA_CACERTS" "$TRUSTSTORE_PATH"
else
    echo -e "${YELLOW}Warning: Could not find system cacerts, creating new truststore...${NC}"
    # Create empty truststore
    keytool -genkeypair -alias temp -keyalg RSA -keystore "$TRUSTSTORE_PATH" \
        -storepass "$TRUSTSTORE_PASSWORD" -keypass "$TRUSTSTORE_PASSWORD" \
        -dname "CN=temp" 2>/dev/null
    keytool -delete -alias temp -keystore "$TRUSTSTORE_PATH" \
        -storepass "$TRUSTSTORE_PASSWORD" 2>/dev/null
fi

# Import the emulator certificate into the truststore
echo -e "${YELLOW}Importing emulator certificate into truststore...${NC}"
keytool -importcert -noprompt \
    -alias cosmos-emulator \
    -file "$CERT_FILE" \
    -keystore "$TRUSTSTORE_PATH" \
    -storepass "$TRUSTSTORE_PASSWORD" 2>/dev/null || true

echo -e "${GREEN}Truststore configured at: $TRUSTSTORE_PATH${NC}"

# Set Cosmos DB emulator environment variables
# Note: The emulator uses a well-known key for local development
# Use HTTPS on port 8081 (management/data explorer port) instead of HTTP on port 1234
export COSMOS_ENDPOINT="https://localhost:8081"
export COSMOS_KEY="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
export COSMOS_DATABASE="test_chronon_db"

# Set Java options to use the custom truststore
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=$TRUSTSTORE_PATH -Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASSWORD"

echo -e "${GREEN}Cosmos DB connection settings:${NC}"
echo -e "${GREEN}  COSMOS_ENDPOINT=$COSMOS_ENDPOINT${NC}"
echo -e "${GREEN}  COSMOS_DATABASE=$COSMOS_DATABASE${NC}"
echo -e "${GREEN}  Truststore: $TRUSTSTORE_PATH${NC}"

# Navigate to project root (script is in cloud_azure/ subdirectory)
cd "$(dirname "$0")/.." || exit

# Run the tests
echo -e "${GREEN}=== Running Cosmos DB tests ===${NC}"
if [ $# -eq 0 ]; then
    # Run all tests
    ./mill cloud_azure.test
else
    # Pass arguments directly to mill (e.g., for testOnly)
    # Usage: ./run-cosmos-tests.sh testOnly "ai.chronon.integrations.cloud_azure.CosmosKVStoreTest"
    ./mill cloud_azure.test."$@"
fi

TEST_EXIT_CODE=$?

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}=== All tests passed! ===${NC}"
else
    echo -e "${RED}=== Tests failed with exit code $TEST_EXIT_CODE ===${NC}"
fi

exit $TEST_EXIT_CODE
