load("@rules_jvm_external//:defs.bzl", "artifact")

# Hardcoded Scala versions for bzlmod migration
_SCALA_MAJOR_VERSION = "2.12"

def jar(org, name, rev = None, classifier = None):
    if rev:
        fail("Passing rev is no longer supported in jar() and scala_jar()")
    rev = ""
    if classifier:
        return "{}:{}:jar:{}:{}".format(org, name, classifier, rev)
    else:
        return "{}:{}:{}".format(org, name, rev)

def scala_jar(org, name, rev = None, classifier = None):
    name = "{}_{}".format(name, _SCALA_MAJOR_VERSION)
    return jar(org, name, rev, classifier)
