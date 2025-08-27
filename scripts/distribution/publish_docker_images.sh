#!/bin/bash


if [[ -n $(git diff HEAD) ]]; then
    echo "Error: You have uncommitted changes. Please commit and push them to git so we can track them."
    exit 1
fi

# Get current branch name
local_branch=$(git rev-parse --abbrev-ref HEAD)

# Fetch latest from remote
git fetch origin $local_branch

# Check if local is behind remote
if [[ -n $(git diff HEAD..origin/$local_branch) ]]; then
    echo "Error: Your branch is not in sync with remote"
    echo "Please push your local changes and sync your local branch $local_branch with remote"
    exit 1
fi

set -e

SCRIPT_DIRECTORY=$(dirname -- "$(realpath -- "$0")")
CHRONON_ROOT_DIR=$(dirname "$(dirname "$SCRIPT_DIRECTORY")")

echo "Working in $CHRONON_ROOT_DIR"
cd $CHRONON_ROOT_DIR

echo "Building jars"

./mill cloud_gcp.assembly
./mill cloud_aws.assembly
./mill service.assembly

CLOUD_GCP_JAR="$CHRONON_ROOT_DIR/out/cloud_gcp/assembly.dest/out.jar"
CLOUD_AWS_JAR="$CHRONON_ROOT_DIR/out/cloud_aws/assembly.dest/out.jar"
SERVICE_JAR="$CHRONON_ROOT_DIR/out/service/assembly.dest/out.jar"

if [ ! -f "$CLOUD_GCP_JAR" ]; then
    echo "$CLOUD_GCP_JAR not found"
    exit 1
fi

if [ ! -f "$SERVICE_JAR" ]; then
    echo "$SERVICE_JAR not found"
    exit 1
fi

if [ ! -f "$CLOUD_AWS_JAR" ]; then
    echo "$CLOUD_AWS_JAR not found"
    exit 1
fi

# Copy jars to build output for docker build
echo "Copying jars to build_output"
mkdir -p build_output
cp out/service/assembly.dest/out.jar build_output/service_assembly_deploy.jar
cp out/cloud_aws/assembly.dest/out.jar build_output/cloud_aws_lib_deploy.jar
cp out/cloud_gcp/assembly.dest/out.jar build_output/cloud_gcp_lib_deploy.jar

echo "Kicking off a docker login"
docker login

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/fetcher/Dockerfile \
  -t ziplineai/chronon-fetcher:$(git rev-parse --short HEAD) \
  -t ziplineai/chronon-fetcher:latest \
  --push \
  .

# Clean up build output dir
rm -rf build_output
