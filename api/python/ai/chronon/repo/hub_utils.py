import importlib
import inspect
import os
from dataclasses import dataclass

from ai.chronon.orchestration.ttypes import Conf
from ai.chronon.repo import FOLDER_NAME_TO_CLASS, OUTPUT_ROOT, gitpython_utils
from ai.chronon.repo.hub_uploader import compute_and_upload_diffs
from ai.chronon.utils import get_mod_and_var_name_from_gc


@dataclass
class ConfigDetails:
    module: str
    variable: str
    file: str
    compiled_file: str
    chronon_root: str
    output_root: str

    def __init__(self, obj):

        # Get object type from FOLDER_NAME_TO_CLASS
        try:
            obj_type = next(
                k for k, v in FOLDER_NAME_TO_CLASS.items() if isinstance(obj, v)
            )
        except StopIteration:
            valid_types = [cls.__name__ for cls in FOLDER_NAME_TO_CLASS.values()]
            raise ValueError(
                f"Can only run one of {valid_types}, got {type(obj).__name__}"
            ) from None

        # Get module and variable name
        self.module, self.variable = get_mod_and_var_name_from_gc(obj, obj_type)

        if self.module is None or self.variable is None:
            raise ValueError("Could not determine module and variable name for object")

        # Get file path
        module = importlib.import_module(self.module)
        self.file = inspect.getmodule(module).__file__
        if not self.file:
            raise ValueError(
                f"""
                                Could not determine file location for module {self.module}, {self.variable}.\n
                                Runner currently only supports working on files saved within a valid Chronon
                                root directory.\n Make sure you have your Zipline python files within the
                                right directory, and then you can import them to your desired runtime.
                             """
            )

        # Validate module path
        path_parts = self.module.split(".")
        if path_parts[0] != obj_type:
            raise ValueError(
                f"Expected module path to start with '{obj_type}', got {self.module}"
            )

        # Get chronon root and build compiled file path
        self.chronon_root = _get_chronon_root(self.file)
        self.output_root = f"{self.chronon_root}/{OUTPUT_ROOT}"
        team = path_parts[1]
        path = ".".join(path_parts[2:])
        self.compiled_file = (
            f"{self.output_root}/{obj_type}/{team}/{path}.{self.variable}"
        )


def _get_chronon_root(filepath):
    """
    Infer chronon root from a filepath to a Chronon object
    """
    target_dirs = FOLDER_NAME_TO_CLASS.keys()

    current_path = os.path.dirname(os.path.abspath(filepath))

    while current_path != os.path.dirname(current_path):  # Stop at root directory
        dir_name = os.path.basename(current_path)
        if dir_name in target_dirs:
            return os.path.dirname(current_path)
        current_path = os.path.dirname(current_path)

    raise ValueError(
        f"{filepath} is not within a valid Chronon root directory, containing {target_dirs} subdirs."
    )


def get_branch(path):
    """
    Get the git branch for the given path.
    """

    try:
        default_branch = gitpython_utils.get_default_origin_branch(path)
        current_branch = gitpython_utils.get_current_branch(path)
    except Exception as e:
        raise ValueError(
            f"Failed to get git branch for {path}. Make sure your Chronon directory is in a git repository."
        ) from e
    if current_branch == default_branch:
        raise RuntimeError(
            f"You're currently on the production branch {default_branch}, please checkout a new branch"
            + "before running to ensure that your changes do not interfere with production."
        )

    else:
        print(f"Identified branch: {current_branch}")
        return current_branch




def upload_to_branch(chronon_root, zipline_hub, local_repo_entities: dict[str, Conf]):
    """
    Determines the correct current branch, uploads the state to the remote branch,
    and returns the branch name
    """
    branch = get_branch(chronon_root)
    return compute_and_upload_diffs(branch, zipline_hub, local_repo_entities)


def info(self, branch=None):
    """
    Prints information about a zipline object, including a link to the ZiplineHub page which
    shows additional information. Attached to GroupBy and Join.

    Args:
        branch: Optional git branch to use for getting object info, if none is provided
            will use the user's dev branch (default: None)

    Returns:
        None

    Raises:
        ValueError: If the object cannot be compiled or info cannot be retrieved
    """
    upload_to_branch(self)
    print("\n\n TODO -- Implement \n\n")


def fetch(self, branch=None):
    return
