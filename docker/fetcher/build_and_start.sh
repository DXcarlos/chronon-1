#!/bin/bash

# Chronon Fetcher - Build and Start Script
# This script builds the necessary JARs, creates the Docker image, and starts the service

set -e

SCRIPT_DIR=$(dirname -- "$(realpath -- "$0")")
CHRONON_ROOT_DIR=$(dirname "$(dirname "$SCRIPT_DIR")")

echo "========================================"
echo "Chronon Fetcher - Build and Start"
echo "========================================"
echo ""
echo "Working directory: $CHRONON_ROOT_DIR"
cd "$CHRONON_ROOT_DIR"

# Parse command line arguments
SKIP_BUILD=false
DETACH=false
REBUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --rebuild)
            REBUILD=true
            shift
            ;;
        -d|--detach)
            DETACH=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --skip-build    Skip JAR compilation (use existing build_output)"
            echo "  --rebuild       Clean and rebuild Docker image"
            echo "  -d, --detach    Run containers in detached mode"
            echo "  -h, --help      Show this help message"
            echo ""
            echo "Environment Variables (can be set in .env file):"
            echo "  GCP_PROJECT_ID                    - GCP project ID"
            echo "  GCP_BIGTABLE_INSTANCE_ID          - GCP Bigtable instance ID"
            echo "  GOOGLE_APPLICATION_CREDENTIALS    - Path to GCP credentials JSON"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Step 1: Build JARs (unless skipped)
if [[ "$SKIP_BUILD" == true ]]; then
    echo "⏭️  Skipping JAR build (--skip-build flag set)"
    echo ""
else
    echo "📦 Step 1/3: Building JARs with mill..."
    echo "----------------------------------------"

    echo "Building cloud_gcp module..."
    ./mill cloud_gcp.assembly

    echo "Building service module..."
    ./mill service.assembly

    # Check that JARs were created
    CLOUD_GCP_JAR="$CHRONON_ROOT_DIR/out/cloud_gcp/assembly.dest/out.jar"
    SERVICE_JAR="$CHRONON_ROOT_DIR/out/service/assembly.dest/out.jar"

    if [ ! -f "$CLOUD_GCP_JAR" ]; then
        echo "❌ Error: $CLOUD_GCP_JAR not found"
        exit 1
    fi

    if [ ! -f "$SERVICE_JAR" ]; then
        echo "❌ Error: $SERVICE_JAR not found"
        exit 1
    fi

    echo "✅ JARs built successfully"
    echo ""
fi

# Step 2: Prepare build_output directory
echo "📁 Step 2/3: Preparing build_output directory..."
echo "----------------------------------------"

# Create build_output directory
mkdir -p build_output

# Copy JARs to build_output for Docker build
echo "Copying JARs to build_output..."
cp out/service/assembly.dest/out.jar build_output/service_assembly_deploy.jar
cp out/cloud_gcp/assembly.dest/out.jar build_output/cloud_gcp_lib_deploy.jar

echo "✅ Build output prepared"
echo ""

# Step 3: Build Docker image
echo "🐳 Step 3/3: Building Docker image..."
echo "----------------------------------------"

if [[ "$REBUILD" == true ]]; then
    echo "Rebuilding Docker image (no cache)..."
    docker build --no-cache \
      -f docker/fetcher/Dockerfile \
      -t ziplineai/chronon-fetcher:latest \
      .
else
    echo "Building Docker image..."
    docker build \
      -f docker/fetcher/Dockerfile \
      -t ziplineai/chronon-fetcher:latest \
      .
fi

echo "✅ Docker image built successfully"
echo ""

# Clean up build_output directory
echo "🧹 Cleaning up build_output directory..."
rm -rf build_output
echo ""

# Step 4: Start Docker service
echo "🚀 Starting Chronon Fetcher service..."
echo "----------------------------------------"

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "⚠️  Warning: .env file not found in $SCRIPT_DIR"
    echo "   Creating .env from .env.example..."
    if [ -f "$SCRIPT_DIR/.env.example" ]; then
        cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
        echo "   Please edit $SCRIPT_DIR/.env with your configuration"
    else
        echo "   .env.example not found. Please create .env manually."
    fi
fi

# Create docker-compose.yml with the specified configuration
cat > "$SCRIPT_DIR/docker-compose-debug.yml" << 'EOF'
services:
  chronon-fetcher:
    image: ziplineai/chronon-fetcher:latest
    restart: "no"
    ports:
      - "9000:9000"
      - "8907:8905"
      - "8908:8906"
      - "5005:5005"  # Debug port
    volumes:
      - ${GOOGLE_APPLICATION_CREDENTIALS:-~/.config/gcloud/application_default_credentials.json}:${GOOGLE_APPLICATION_CREDENTIALS:-/gcp/credentials.json}:ro
    environment:
      - GCP_PROJECT_ID=${GCP_PROJECT_ID}
      - GOOGLE_CLOUD_PROJECT=${GOOGLE_CLOUD_PROJECT:-${GCP_PROJECT_ID}}
      - GCP_BIGTABLE_INSTANCE_ID=${GCP_BIGTABLE_INSTANCE_ID}
      - GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-/gcp/credentials.json}
      - JVM_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
      # Bigtable RPC timeout settings (increased from 200ms default to 1s as recommended by Google)
      - BIGTABLE_INITIAL_RPC_TIMEOUT_DURATION=PT1S
      - BIGTABLE_MAX_RPC_TIMEOUT_DURATION=PT2S
      - BIGTABLE_TOTAL_TIMEOUT_DURATION=PT5S
EOF

echo "✅ Created docker-compose-debug.yml"
echo ""

# Start the service
cd "$SCRIPT_DIR"

if [[ "$DETACH" == true ]]; then
    echo "Starting service in detached mode..."
    docker compose -f docker-compose-debug.yml --env-file .env up -d
    echo ""
    echo "✅ Service started in background"
    echo ""
    echo "📊 View logs with:"
    echo "   docker compose -f docker/fetcher/docker-compose-debug.yml logs -f"
else
    echo "Starting service (press Ctrl+C to stop)..."
    docker compose -f docker-compose-debug.yml --env-file .env up
fi

echo ""
echo "========================================"
echo "Service Information"
echo "========================================"
echo "Main API:         http://localhost:9000"
echo "Prometheus (Chronon): http://localhost:8907/metrics"
echo "Prometheus (Vert.x):  http://localhost:8908/metrics"
echo "Debug port:       localhost:5005"
echo ""
echo "Test the service:"
echo "  curl http://localhost:9000/ping"
echo "  curl http://localhost:9000/v1/joins"
echo ""
echo "Stop the service:"
echo "  docker compose -f docker/fetcher/docker-compose-debug.yml down"
echo "========================================"
