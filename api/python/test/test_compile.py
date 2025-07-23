import os
from unittest.mock import MagicMock, patch

from ai.chronon.api.ttypes import GroupBy, MetaData
from ai.chronon.cli.compile import parse_configs
from ai.chronon.cli.compile.compile_context import CompileContext
from ai.chronon.repo.compile import __compile


def test_compile(repo):
    os.chdir(repo)
    results = __compile(chronon_root=repo)
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
