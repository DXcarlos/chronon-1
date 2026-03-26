#!/bin/bash

set -e

# Parse command line arguments
function print_usage() {
    echo "Usage: $0 --cloud <cloud> --version <version> [OPTIONS]"
    echo ""
    echo "Required arguments:"
    echo "  --cloud <cloud>      Cloud provider (gcp, aws, azure, or all)"
    echo "  --version <version>  Version tag for the Docker images"
    echo ""
    echo "Optional arguments:"
    echo "  --skip-build         Skip building JARs (use existing ones)"
    echo "  --skip-push          Build images but don't push to Docker Hub"
    echo "  --build-fetcher      Also build and push the fetcher image"
    echo "  --platform <platform> Target platform (default: linux/amd64)"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --cloud gcp --version 0.1.0"
    echo "  $0 --cloud all --version 0.2.0 --build-fetcher"
    echo "  $0 --cloud aws --version dev --skip-push"
}

CLOUD=""
VERSION=""
SKIP_BUILD=false
SKIP_PUSH=false
BUILD_FETCHER=false
PLATFORM="linux/amd64"

while [[ $# -gt 0 ]]; do
    case $1 in
        --cloud)
            CLOUD="$2"
            shift 2
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-push)
            SKIP_PUSH=true
            shift
            ;;
        --build-fetcher)
            BUILD_FETCHER=true
            shift
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [[ -z "$CLOUD" ]]; then
    echo "Error: --cloud is required"
    print_usage
    exit 1
fi

if [[ -z "$VERSION" ]]; then
    echo "Error: --version is required"
    print_usage
    exit 1
fi

# Validate cloud parameter
if [[ "$CLOUD" != "gcp" && "$CLOUD" != "aws" && "$CLOUD" != "azure" && "$CLOUD" != "all" ]]; then
    echo "Error: --cloud must be one of: gcp, aws, azure, all"
    exit 1
fi

SCRIPT_DIRECTORY=$(dirname -- "$(realpath -- "$0")")
CHRONON_ROOT_DIR=$(dirname "$(dirname "$SCRIPT_DIRECTORY")")

echo "Working in $CHRONON_ROOT_DIR"
cd "$CHRONON_ROOT_DIR"

# Determine which clouds to build
if [[ "$CLOUD" == "all" ]]; then
    CLOUDS=("gcp" "aws" "azure")
else
    CLOUDS=("$CLOUD")
fi

# Build JARs if not skipped
if [[ "$SKIP_BUILD" == false ]]; then
    echo "=========================================="
    echo "Building JARs..."
    echo "=========================================="

    # Always build common JARs
    echo "Building common JARs (service, flink)..."
    ./mill service.assembly
    ./mill flink.assembly

    # Build cloud-specific JARs
    for cloud_name in "${CLOUDS[@]}"; do
        echo "Building $cloud_name JARs..."

        case "$cloud_name" in
            gcp)
                ./mill cloud_gcp.assembly
                ./mill flink_connectors.pubsub.assembly
                ;;
            aws)
                ./mill cloud_aws.assembly
                ./mill flink_connectors.kinesis.assembly
                ;;
            azure)
                ./mill cloud_azure.assembly
                ;;
        esac
    done

    echo "JAR builds completed successfully"
else
    echo "Skipping JAR builds (using existing JARs)"
fi

# Verify JARs exist
echo "=========================================="
echo "Verifying JAR files..."
echo "=========================================="

SRC_SERVICE_JAR="$CHRONON_ROOT_DIR/out/service/assembly.dest/out.jar"
SRC_FLINK_JAR="$CHRONON_ROOT_DIR/out/flink/assembly.dest/out.jar"

if [[ ! -f "$SRC_SERVICE_JAR" ]]; then
    echo "Error: $SRC_SERVICE_JAR not found"
    exit 1
fi

if [[ ! -f "$SRC_FLINK_JAR" ]]; then
    echo "Error: $SRC_FLINK_JAR not found"
    exit 1
fi

for cloud_name in "${CLOUDS[@]}"; do
    case "$cloud_name" in
        gcp)
            SRC_CLOUD_JAR="$CHRONON_ROOT_DIR/out/cloud_gcp/assembly.dest/out.jar"
            SRC_CONNECTOR_JAR="$CHRONON_ROOT_DIR/out/flink_connectors/pubsub/assembly.dest/out.jar"
            ;;
        aws)
            SRC_CLOUD_JAR="$CHRONON_ROOT_DIR/out/cloud_aws/assembly.dest/out.jar"
            SRC_CONNECTOR_JAR="$CHRONON_ROOT_DIR/out/flink_connectors/kinesis/assembly.dest/out.jar"
            ;;
        azure)
            SRC_CLOUD_JAR="$CHRONON_ROOT_DIR/out/cloud_azure/assembly.dest/out.jar"
            SRC_CONNECTOR_JAR=""  # Azure doesn't have a connector
            ;;
    esac

    if [[ ! -f "$SRC_CLOUD_JAR" ]]; then
        echo "Error: $SRC_CLOUD_JAR not found"
        exit 1
    fi

    if [[ -n "$SRC_CONNECTOR_JAR" && ! -f "$SRC_CONNECTOR_JAR" ]]; then
        echo "Error: $SRC_CONNECTOR_JAR not found"
        exit 1
    fi
done

echo "All required JARs verified"

# Docker login if not skipping push
if [[ "$SKIP_PUSH" == false ]]; then
    echo "=========================================="
    echo "Logging in to Docker Hub..."
    echo "=========================================="
    docker login
fi

# Build engine images for each cloud
echo "=========================================="
echo "Building Engine Docker Images..."
echo "=========================================="

for cloud_name in "${CLOUDS[@]}"; do
    echo ""
    echo "Building engine image for $cloud_name..."

    # Prepare build_output directory with cloud-specific JARs
    mkdir -p build_output/engine

    case "$cloud_name" in
        gcp)
            cp "$CHRONON_ROOT_DIR/out/cloud_gcp/assembly.dest/out.jar" build_output/engine/cloud_gcp_lib_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/flink/assembly.dest/out.jar" build_output/engine/flink_assembly_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/service/assembly.dest/out.jar" build_output/engine/service_assembly_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/flink_connectors/pubsub/assembly.dest/out.jar" build_output/engine/connectors_pubsub_deploy.jar
            ;;
        aws)
            cp "$CHRONON_ROOT_DIR/out/cloud_aws/assembly.dest/out.jar" build_output/engine/cloud_aws_lib_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/flink/assembly.dest/out.jar" build_output/engine/flink_assembly_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/service/assembly.dest/out.jar" build_output/engine/service_assembly_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/flink_connectors/kinesis/assembly.dest/out.jar" build_output/engine/connectors_kinesis_deploy.jar
            ;;
        azure)
            cp "$CHRONON_ROOT_DIR/out/cloud_azure/assembly.dest/out.jar" build_output/engine/cloud_azure_lib_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/flink/assembly.dest/out.jar" build_output/engine/flink_assembly_deploy.jar
            cp "$CHRONON_ROOT_DIR/out/service/assembly.dest/out.jar" build_output/engine/service_assembly_deploy.jar
            ;;
    esac

    # Create engine manifest
    COMMIT_SHA=$(git rev-parse HEAD)
    echo "{\"version\": \"$VERSION\", \"cloud\": \"$cloud_name\", \"commit\": \"$COMMIT_SHA\"}" > build_output/engine_manifest.json

    # Build Docker image
    if [[ "$SKIP_PUSH" == false ]]; then
        docker buildx build \
            --platform "$PLATFORM" \
            --build-arg VERSION="$VERSION" \
            -f docker/engine/Dockerfile \
            -t "ziplineai/engine-$cloud_name:$VERSION" \
            --push \
            .
        echo "✓ Pushed ziplineai/engine-$cloud_name:$VERSION to Docker Hub"
    else
        docker buildx build \
            --platform "$PLATFORM" \
            --build-arg VERSION="$VERSION" \
            -f docker/engine/Dockerfile \
            -t "ziplineai/engine-$cloud_name:$VERSION" \
            --load \
            .
        echo "✓ Built ziplineai/engine-$cloud_name:$VERSION locally"
    fi

    # Clean up build_output
    rm -rf build_output
done

# Build fetcher image if requested
if [[ "$BUILD_FETCHER" == true ]]; then
    echo ""
    echo "=========================================="
    echo "Building Fetcher Docker Image..."
    echo "=========================================="

    # Prepare build_output with all cloud JARs for fetcher
    mkdir -p build_output

    # Fetcher needs all cloud JARs regardless of which clouds were built
    if [[ -f "$CHRONON_ROOT_DIR/out/cloud_gcp/assembly.dest/out.jar" ]]; then
        cp "$CHRONON_ROOT_DIR/out/cloud_gcp/assembly.dest/out.jar" build_output/cloud_gcp_lib_deploy.jar
    else
        echo "Warning: GCP JAR not found, fetcher may not work with GCP"
        touch build_output/cloud_gcp_lib_deploy.jar  # Create empty file to satisfy Dockerfile
    fi

    if [[ -f "$CHRONON_ROOT_DIR/out/cloud_aws/assembly.dest/out.jar" ]]; then
        cp "$CHRONON_ROOT_DIR/out/cloud_aws/assembly.dest/out.jar" build_output/cloud_aws_lib_deploy.jar
    else
        echo "Warning: AWS JAR not found, fetcher may not work with AWS"
        touch build_output/cloud_aws_lib_deploy.jar
    fi

    if [[ -f "$CHRONON_ROOT_DIR/out/cloud_azure/assembly.dest/out.jar" ]]; then
        cp "$CHRONON_ROOT_DIR/out/cloud_azure/assembly.dest/out.jar" build_output/cloud_azure_lib_deploy.jar
    else
        echo "Warning: Azure JAR not found, fetcher may not work with Azure"
        touch build_output/cloud_azure_lib_deploy.jar
    fi

    cp "$SRC_SERVICE_JAR" build_output/service_assembly_deploy.jar

    if [[ "$SKIP_PUSH" == false ]]; then
        docker buildx build \
            --platform "$PLATFORM" \
            -f docker/fetcher/Dockerfile \
            -t "ziplineai/chronon-fetcher:$VERSION" \
            -t "ziplineai/chronon-fetcher:latest" \
            --push \
            .
        echo "✓ Pushed ziplineai/chronon-fetcher:$VERSION to Docker Hub"
    else
        docker buildx build \
            --platform "$PLATFORM" \
            -f docker/fetcher/Dockerfile \
            -t "ziplineai/chronon-fetcher:$VERSION" \
            -t "ziplineai/chronon-fetcher:latest" \
            --load \
            .
        echo "✓ Built ziplineai/chronon-fetcher:$VERSION locally"
    fi

    # Clean up build_output
    rm -rf build_output
fi

echo ""
echo "=========================================="
echo "Build Complete!"
echo "=========================================="
echo "Images built for clouds: ${CLOUDS[*]}"
echo "Version: $VERSION"
if [[ "$SKIP_PUSH" == false ]]; then
    echo "Status: Pushed to Docker Hub"
else
    echo "Status: Built locally (not pushed)"
fi