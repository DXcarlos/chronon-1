load("@rules_jvm_external//:defs.bzl", "artifact")
load("@rules_jvm_external//:specs.bzl", "parse")
load("//tools/build_rules:utils.bzl", "flatten", "map")

def _artifact_string(group, artifact, version, exclusions):
    result = parse.parse_maven_coordinate("{}:{}:{}".format(group, artifact, version))
    if (exclusions != None):
        result["exclusions"] = exclusions
    return result

def versioned_artifacts(group, version, artifacts, exclusions = None):
    return map(lambda artifact: _artifact_string(group, artifact, version, exclusions), artifacts)

def repository(name, pinned = True, artifacts = [], overrides = {}, provided = False, vars = {}, excluded_artifacts = [], maven_install_json = None):
    final_artifacts = []
    flat_artifacts = flatten(artifacts)
    for artifact in parse.parse_artifact_spec_list(flat_artifacts):
        # Empty string in packaging seems to mess up Coursier, maybe a bug in RJE
        if artifact.get("packaging") == "":
            artifact.pop("packaging")
        artifact["version"] = artifact["version"].format(**vars)
        final_artifacts.append(artifact)
    return struct(
        name = name,
        pinned = pinned,
        artifacts = final_artifacts,
        overrides = overrides,
        provided = provided,
        vars = vars,
        excluded_artifacts = excluded_artifacts,
        maven_install_json = maven_install_json,
    )

def get_jars_for_repository(repo_name, jars):
    return [artifact(jar, repository_name = repo_name) for jar in jars]
