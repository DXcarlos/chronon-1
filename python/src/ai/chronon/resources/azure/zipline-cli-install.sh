#!/bin/bash

function print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --artifact_prefix <azure storage container>     Specify the azure storage container to upload artifacts to e.g. \"https://ziplineai2.blob.core.windows.net/dev-zipline-artifacts\""
    echo "  --version <version>                 Specify the version you want to run"
    echo "  -h, --help  Show this help message"
}

if [ $# -ne 4 ]; then
    print_usage
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --artifact_prefix)
            if [[ -z $2 ]]; then
                echo "Error: --artifact_prefix requires a value"
                print_usage
                exit 1
            fi
            ARTIFACT_PREFIX="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        --version)
            if [[ -z $2 ]]; then
                echo "Error: --version requires a value"
                print_usage
                exit 1
            fi
            VERSION="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

az storage blob download \
  --blob-url "${ARTIFACT_PREFIX%/}/release/$VERSION/wheels/zipline_ai-$VERSION-py3-none-any.whl" \
  -f zipline_ai-$VERSION-py3-none-any.whl \
  --auth-mode login

pip3 uninstall --yes zipline-ai

pip3 install --no-input ./zipline_ai-$VERSION-py3-none-any.whl

trap 'rm -f ./zipline_ai-$VERSION-py3-none-any.whl' EXIT