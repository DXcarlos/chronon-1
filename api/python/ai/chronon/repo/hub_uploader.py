import glob
import hashlib
import json
import os

from ai.chronon.orchestration.ttypes import Conf
from ai.chronon.repo import (
    FOLDER_NAME_TO_CLASS,
    FOLDER_NAME_TO_LOGICAL_TYPE,
)
from ai.chronon.repo.zipline_hub import ZiplineHub


def _get_diffed_entities(root_dir: str, branch: str, zipline_hub: ZiplineHub):
    compiled_dir = os.path.join(root_dir, "compiled")
    local_repo_entities = _build_local_repo_hashmap(compiled_dir)

    # Call Zipline hub with `names_and_hashes` as the argument to get back
    names_and_hashes = {name: local_conf.hash for name, local_conf in local_repo_entities.items()}
    changed_entity_names = zipline_hub.call_diff_api(names_and_hashes)

    # a list of names for diffed hashes on branch
    return {k: local_repo_entities[k] for k in changed_entity_names}



def _build_local_repo_hashmap(root_dir: str):
    # Returns a map of name -> (tbinary, file_hash)
    results = {}

    # Iterate through each object type folder (staging_queries, group_bys, joins etc)
    for folder_name, _ in FOLDER_NAME_TO_CLASS.items():
        folder_path = os.path.join(root_dir, folder_name)
        if not os.path.exists(folder_path):
            continue

        # Find all json files recursively in this folder
        json_files = [
            f
            for f in glob.glob(os.path.join(folder_path, "**/*"), recursive=True)
            if os.path.isfile(f)
        ]

        exceptions = []

        for json_file in json_files:
            try:
                # Read the json file
                with open(json_file, "r") as f:
                    thrift_json = f.read()

                # Extract name from metadata in json
                json_obj = json.loads(thrift_json)
                name = json_obj["metaData"]["name"]

                # Load the json into the appropriate object type based on folder
                # binary = json2binary(thrift_json, obj_class)

                md5_hash = hashlib.md5(thrift_json.encode()).hexdigest()
                # md5_hash = hashlib.md5(thrift_json.encode()).hexdigest() + "123"
                # results[name] = (binary, md5_hash, FOLDER_NAME_TO_LOGICAL_TYPE[folder_name])
                logical_type = FOLDER_NAME_TO_LOGICAL_TYPE[folder_name]
                print(f"logical type for this file {json_file} is {logical_type}")
                results[name] = Conf(name=name,
                                          hash=md5_hash,
                                          # contents=binary,
                                          contents=thrift_json,
                                          logicalType=logical_type)

            except Exception as e:
                exceptions.append(f"{json_file} - {e}")

        if exceptions:
            error_msg = (
                    "The following files had exceptions during upload: \n"
                    + "\n".join(exceptions)
                    + "\n\n Consider deleting the files (safe operation) and checking "
                    + "your thrift version before rerunning your command."
            )
            raise RuntimeError(error_msg)

    return results


def compute_and_upload_diffs(root_dir: str, branch: str, zipline_hub: ZiplineHub):
    diffed_entities = _get_diffed_entities(root_dir, branch, zipline_hub)
    entity_keys_str = "\n".join(diffed_entities.keys())
    log_str = "\n\nUploading:\n{entity_keys}".format(entity_keys=entity_keys_str)
    print(log_str)

    diff_confs = []
    for _, conf in diffed_entities.items():
        diff_confs.append(conf.__dict__)

    # Make PUT request to ZiplineHub
    zipline_hub.call_upload_api(branch=branch, diff_confs=diff_confs)
    return
