set -euxo pipefail

echo "🚀 Building wheel from mill generatedSources..."

echo "🔧 Generating thrift sources..."
./mill python.generatedSources

TEMP_DIR=$(mktemp -d)
echo "Using temp directory: $TEMP_DIR"

# Clean up on exit
trap "rm -rf $TEMP_DIR" EXIT

cp -r python/ $TEMP_DIR/
# move the python sources one level up
ls $TEMP_DIR/python/
mv $TEMP_DIR/src/* $TEMP_DIR/

echo "📋 Copying generated sources..."
cp -r out/python/generatedSources.dest/gen_thrift $TEMP_DIR/

echo "🔧 Building wheel..."
WHEEL_DIR="$TEMP_DIR/wheels"
mkdir -p "$WHEEL_DIR"

ENV_VERSION=$1
VERSION=$ENV_VERSION pip wheel $TEMP_DIR --wheel-dir $WHEEL_DIR

cp $WHEEL_DIR/zipline_ai-*.whl .

WHEEL_FILE=`ls zipline_ai-*.whl`
if [ -n "$WHEEL_FILE" ]; then
    echo "✅ SUCCESS! Wheel created: $WHEEL_FILE"
else
    echo "❌ No wheel created"
    exit 1
fi
