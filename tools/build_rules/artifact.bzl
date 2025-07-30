load("@rules_jvm_external//:defs.bzl", _rje_artifact = "artifact")
load("@rules_java//java:defs.bzl", "java_import")
load("//tools/build_rules:jar_library.bzl", "jar_library")
# Maven repository name for bzlmod
MAVEN_REPOSITORY_NAME = "maven"

# Hardcoded Scala versions for bzlmod migration
_SCALA_VERSION = "2.12.20"
_SCALA_MAJOR_VERSION = "2.12"

# Converts to bazel friendly target name specification with underscores
def get_safe_name(coord):
    return coord.replace(":", "_").replace(".", "_").replace("-", "_")

def _get_artifact(coord, repository_name):
    """
    Helper macro to translate Maven coordinates into Bazel deps. Example:
    java_library(
        name = "foo",
        srcs = ["Foo.java"],
        deps = [maven_artifact("com.google.guava:guava")],
    )
    Arguments:
        repository_name: If provided, always fetch from this Maven repo instead of determining
        the repo automatically. Be careful when using this as Bazel will not prevent multiple
        jars from providing the same class on the classpath, in which case the order of "deps"
        will determine which one "wins".
    """
    if repository_name:
        return _rje_artifact(coord, repository_name = repository_name)

    safe_name = get_safe_name(coord)

    if not native.existing_rule(safe_name):
        jar_library(
            name = safe_name,
            jars = [coord],
            visibility = ["//visibility:private"],
            tags = ["manual"],
        )
    return safe_name

# For specifying dependencies pulled from Maven Repository in our build targets
# Example: maven_artifact("com.google.guava:guava")
def maven_artifact(coord):
    return _get_artifact(coord, MAVEN_REPOSITORY_NAME)

def maven_artifact_with_suffix(coord):
    full_coord = coord + "_" + _SCALA_MAJOR_VERSION
    return _get_artifact(full_coord, MAVEN_REPOSITORY_NAME)

def scala_artifact(coord):
    # In bzlmod, route scala artifacts to the main maven repository
    return _get_artifact(coord, MAVEN_REPOSITORY_NAME)

