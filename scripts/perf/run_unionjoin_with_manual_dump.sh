#!/bin/bash

# UnionJoinTest with Manual Heap Dump Generation
# TODO: Update this script to work with Mill instead of Bazel

echo "This script needs to be updated to work with Mill build system."
echo "The Bazel-specific test runner is no longer available."
echo "Use 'mill spark.test' to run Spark tests or specific test classes."
exit 1

# TODO: Implement Mill equivalent
# cd /Users/nsimha/repos/chronon

# Build the test target
# echo "Building UnionJoinTest..."
# mill spark.test

# Get the generated script path
BAZEL_SCRIPT="bazel-bin/spark/join_test_test_suite_src_test_scala_ai_chronon_spark_test_join_UnionJoinTest.scala"

if [[ ! -f "$BAZEL_SCRIPT" ]]; then
    echo "Error: Bazel script not found at $BAZEL_SCRIPT"
    exit 1
fi

# Set up runfiles directory for classpath resolution  
RUNFILES_DIR="$(pwd)/bazel-bin/spark/join_test_test_suite_src_test_scala_ai_chronon_spark_test_join_UnionJoinTest.scala.runfiles"
export JAVA_RUNFILES="$RUNFILES_DIR"
export TEST_SRCDIR="$RUNFILES_DIR"

# Extract and expand classpath
RUNPATH="${RUNFILES_DIR}/chronon/"
RUNPATH="${RUNPATH#$PWD/}"
RAW_CLASSPATH=$(sed -n '249p' "$BAZEL_SCRIPT" | cut -d'"' -f2)
CLASSPATH=$(echo "$RAW_CLASSPATH" | sed "s|\${RUNPATH}|$RUNPATH|g")

if [[ -z "$CLASSPATH" ]]; then
    echo "Error: Failed to extract classpath"
    exit 1
fi

echo "Successfully extracted classpath (${#CLASSPATH} characters)"

# Create heap dump file path
HEAP_DUMP_FILE="/Users/nsimha/repos/chronon/.ijwb/unionjoin-heapdump-$(date +%Y%m%d-%H%M%S).hprof"
echo "Heap dump will be saved to: $HEAP_DUMP_FILE"

# JVM settings with more aggressive heap dump options
JVM_OPTS="-Xmx8g -Xms2g"  # Reduced max heap to force memory pressure
MODULE_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
SYSTEM_PROPS="-DRULES_SCALA_MAIN_WS_NAME=chronon -DRULES_SCALA_ARGS_FILE=spark/join_test_test_suite_src_test_scala_ai_chronon_spark_test_join_UnionJoinTest.scala.args"

# Heap dump and GC options
PROFILER_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$HEAP_DUMP_FILE -XX:+PrintGC -XX:+PrintGCDetails -XX:+UseG1GC -Xlog:gc*:unionjoin-gc-$(date +%Y%m%d-%H%M%S).log:time"

echo "Starting UnionJoinTest with heap profiling..."
echo "Memory settings: $JVM_OPTS"
echo "Working directory: $(pwd)"
echo ""

# Run in background and get PID for manual heap dump
java \
  $JVM_OPTS \
  $MODULE_OPENS \
  $PROFILER_OPTS \
  $SYSTEM_PROPS \
  -classpath "$CLASSPATH" \
  io.bazel.rulesscala.scala_test.Runner &

JAVA_PID=$!
echo "Java process started with PID: $JAVA_PID"

# Wait a bit for the process to start and consume some memory
echo "Waiting 30 seconds for test to initialize..."
sleep 30

# Check if process is still running
if kill -0 $JAVA_PID 2>/dev/null; then
    echo "Generating manual heap dump..."
    MANUAL_DUMP="/Users/nsimha/repos/chronon/.ijwb/unionjoin-manual-$(date +%Y%m%d-%H%M%S).hprof"
    jcmd $JAVA_PID GC.run_finalization
    jcmd $JAVA_PID VM.gc
    jcmd $JAVA_PID VM.gc
    jcmd $JAVA_PID GC.run_finalization  
    sleep 5
    jcmd $JAVA_PID VM.gc
    echo "Creating heap dump with jcmd..."
    jcmd $JAVA_PID GC.dump_heap $MANUAL_DUMP
    echo "Manual heap dump saved to: $MANUAL_DUMP"
else
    echo "Process already terminated"
fi

# Wait for the test to complete
wait $JAVA_PID
EXIT_CODE=$?

echo ""
echo "Test completed with exit code: $EXIT_CODE"
echo "Checking for heap dump files..."
ls -la /Users/nsimha/repos/chronon/.ijwb/*.hprof 2>/dev/null || echo "No heap dump files found"