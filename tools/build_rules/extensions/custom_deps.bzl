"""Custom dependencies module extension for chronon."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def _zlib_impl(mctx):
    """Implementation for zlib dependency."""
    http_archive(
        name = "zlib",
        sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
        strip_prefix = "zlib-1.2.11",
        urls = [
            "https://mirror.bazel.build/zlib.net/zlib-1.2.11.tar.gz",
            "https://zlib.net/zlib-1.2.11.tar.gz",
        ],
        build_file = "//third_party:zlib.BUILD",
    )

def _custom_deps_impl(mctx):
    """Main implementation for custom dependencies."""
    # Process zlib extensions
    for mod in mctx.modules:
        for zlib in mod.tags.zlib:
            _zlib_impl(mctx)

# Define tags
_zlib = tag_class()

custom_deps = module_extension(
    implementation = _custom_deps_impl,
    tag_classes = {
        "zlib": _zlib,
    },
)