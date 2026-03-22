import os
import json
import tempfile
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner
from gen_thrift.api.ttypes import GroupBy, MetaData

from ai.chronon.cli.compile import parse_configs
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.cli.compile.compiler import Compiler
from ai.chronon.repo.compile import __compile, compile


def test_compile(repo):
    import sys
    sys.path.append(repo)
    results = __compile(chronon_root=repo, ignore_python_errors=True)
    assert len(results) != 0


def test_compile_gcp_resources(gcp_resources):
    """Test compilation of GCP resource examples."""
    import sys
    sys.path.insert(0, gcp_resources)
    results = __compile(chronon_root=gcp_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_aws_resources(aws_resources):
    """Test compilation of AWS resource examples."""
    import sys
    sys.path.insert(0, aws_resources)
    results = __compile(chronon_root=aws_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_compile_azure_resources(azure_resources):
    """Test compilation of Azure resource examples."""
    import sys
    sys.path.insert(0, azure_resources)
    results = __compile(chronon_root=azure_resources, ignore_python_errors=True)
    assert len(results) != 0

def test_parse_configs_relative_source_file():
    """Test that sourceFile is stored as a path relative to chronon_root."""
    # Setup
    test_root = "/fake/root/path"
    test_file_path = "/fake/root/path/group_bys/team/test_group_by.py"
    test_input_dir = os.path.join(test_root, "group_bys")

    # Create a properly initialized GroupBy object with MetaData
    mock_obj = GroupBy()
    mock_obj.metaData = MetaData()

    # Create mock context
    mock_compile_context = MagicMock(spec=CompileContext)
    mock_compile_context.chronon_root = test_root
    mock_compile_context.teams_dict = {}
    mock_compile_context.validator = MagicMock()
    mock_compile_context.validator.validate_obj.return_value = []
    mock_compile_context.compile_status = MagicMock()

    # Configure mocks
    with patch('ai.chronon.cli.compile.parse_configs.from_file') as mock_from_file, \
         patch('ai.chronon.cli.compile.serializer.thrift_simple_json') as mock_serialize, \
         patch('glob.glob', return_value=[test_file_path]), \
         patch('ai.chronon.cli.compile.parse_teams.update_metadata'), \
         patch('ai.chronon.cli.compile.parse_configs.populate_column_hashes'):

        # Configure mock return values
        mock_from_file.return_value = {"team.test_group_by.test_var": mock_obj}
        mock_serialize.return_value = "{}"

        # Call the function being tested
        results = parse_configs.from_folder(GroupBy, test_input_dir, mock_compile_context)

    # Assertions
    assert len(results) == 1
    assert results[0].obj is not None
    assert hasattr(results[0].obj, 'metaData')
    assert results[0].obj.metaData is not None

    # The sourceFile should be a relative path from chronon_root
    expected_relative_path = "group_bys/team/test_group_by.py"
    assert results[0].obj.metaData.sourceFile == expected_relative_path
    assert not results[0].obj.metaData.sourceFile.startswith("/")  # Should be relative, not absolute


def test_compile_with_json_format(canary):
    """Test that compile command with --format json returns valid JSON output."""
    import sys
    sys.path.append(canary)

    runner = CliRunner()
    result = runner.invoke(compile, [
        '--chronon-root', canary,
        '-f', 'json',
        '--ignore-python-errors',
        '--force',
    ])

    # Check that the command executed successfully
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify that the output is valid JSON (should be clean JSON with no tracebacks)
    try:
        output_json = json.loads(result.output)
    except json.JSONDecodeError as e:
        assert False, f"Output is not valid JSON: {result.output}\nError: {e}"

    # Verify the JSON structure contains expected fields
    assert "status" in output_json, f"Output missing 'status' field: {output_json}"
    assert output_json["status"] == "success", f"Expected status 'success', got: {output_json['status']}"

    assert "results" in output_json, f"Output missing 'results' field: {output_json}"
    assert isinstance(output_json["results"], dict), f"'results' should be a dict, got: {type(output_json['results'])}"


# ---------------------------------------------------------------------------
# Duplicate filename detection tests
# ---------------------------------------------------------------------------

def _make_compiler_with_dirs(root, dirs):
    """
    Build a Compiler whose config_infos point at the given dirs dict
    {folder_name: [relative_file_paths]}.  Files are created on disk;
    CompileContext internals are mocked so no real compilation runs.
    """
    for folder, files in dirs.items():
        for rel in files:
            path = os.path.join(root, folder, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").close()

    ctx = MagicMock(spec=CompileContext)
    ctx.config_infos = []

    from gen_thrift.api.ttypes import GroupBy, Join, StagingQuery, Model, ModelTransforms, MetaData
    from ai.chronon.cli.compile.compile_context import ConfigInfo
    from gen_thrift.api.ttypes import ConfType

    folder_to_cls = {
        "group_bys": GroupBy,
        "joins": Join,
        "staging_queries": StagingQuery,
        "models": Model,
        "model_transforms": ModelTransforms,
        "teams_metadata": MetaData,
    }
    for folder in dirs:
        cls = folder_to_cls[folder]
        ci = ConfigInfo(folder_name=folder, cls=cls, config_type=None)
        ctx.config_infos.append(ci)
        ctx.input_dir = lambda c, f=folder, r=root: os.path.join(r, f)

    # Make input_dir dispatch by class
    cls_to_folder = {v: k for k, v in folder_to_cls.items()}
    ctx.input_dir = lambda cls: os.path.join(root, cls_to_folder[cls])

    return Compiler(ctx)


def test_duplicate_filename_across_config_types_raises():
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys": ["gcp/foo.py"],
            "joins":     ["gcp/foo.py"],
        })
        with pytest.raises(click.ClickException) as exc_info:
            compiler._check_duplicate_filenames()
        assert "foo" in str(exc_info.value.format_message())
        assert "gcp" in str(exc_info.value.format_message())


def test_duplicate_filename_different_teams_ok():
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys": ["gcp/foo.py"],
            "joins":     ["aws/foo.py"],
        })
        compiler._check_duplicate_filenames()  # should not raise


def test_duplicate_filename_different_stems_ok():
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys": ["gcp/foo.py"],
            "joins":     ["gcp/bar.py"],
        })
        compiler._check_duplicate_filenames()  # should not raise


def test_duplicate_filename_same_type_ok():
    """Same filename in the same config type (different team dirs) is fine."""
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys": ["gcp/foo.py", "aws/foo.py"],
        })
        compiler._check_duplicate_filenames()  # should not raise


def test_duplicate_filename_nested_path():
    """Conflict only triggers when the full relative-within-team path matches."""
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys": ["gcp/subdir/foo.py"],
            "joins":     ["gcp/subdir/foo.py"],
        })
        with pytest.raises(click.ClickException):
            compiler._check_duplicate_filenames()


def test_duplicate_filename_nested_vs_top_level_ok():
    """gcp/subdir/foo.py and gcp/foo.py are different paths — no conflict."""
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys": ["gcp/subdir/foo.py"],
            "joins":     ["gcp/foo.py"],
        })
        compiler._check_duplicate_filenames()  # should not raise


def test_duplicate_filename_multiple_types_error_message():
    """Error message lists all conflicting config types."""
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys":      ["gcp/foo.py"],
            "joins":          ["gcp/foo.py"],
            "staging_queries": ["gcp/foo.py"],
        })
        with pytest.raises(click.ClickException) as exc_info:
            compiler._check_duplicate_filenames()
        msg = exc_info.value.format_message()
        assert "group_bys" in msg
        assert "joins" in msg
        assert "staging_queries" in msg


def test_duplicate_filename_teams_metadata_ignored():
    """teams_metadata directory is excluded from the duplicate check."""
    with tempfile.TemporaryDirectory() as root:
        compiler = _make_compiler_with_dirs(root, {
            "group_bys":    ["gcp/foo.py"],
            "teams_metadata": ["gcp/foo.py"],
        })
        compiler._check_duplicate_filenames()  # should not raise
