import glob
import hashlib
import json
import os
from collections import defaultdict

from ai.chronon.cli.formatter import Format, format_print
from ai.chronon.cli.theme import print_info, print_step, print_success, print_warning
from ai.chronon.repo import (
    FOLDER_NAME_TO_CLASS,
    FOLDER_NAME_TO_CONF_TYPE,
)
from ai.chronon.repo.zipline_hub import ZiplineHub
from gen_thrift.api.ttypes import Conf


def build_local_repo_hashmap(root_dir: str):
    compiled_dir = os.path.join(root_dir, "compiled")
    # Returns a map of (name, confType) -> Conf to avoid collisions when a conf name
    # is shared across different types (e.g. a join and a model with the same filename).
    results = {}

    # Iterate through each object type folder (staging_queries, group_bys, joins etc)
    for folder_name, _ in FOLDER_NAME_TO_CLASS.items():
        folder_path = os.path.join(compiled_dir, folder_name)
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

                conf_type = FOLDER_NAME_TO_CONF_TYPE[folder_name]
                md5_hash = hashlib.md5(thrift_json.encode()).hexdigest()
                # Key includes the folder name prefix (e.g. "joins#gcp.demo.v1__1") so that
                # same-named confs of different types can coexist in the map.
                results[f"{folder_name}#{name}"] = Conf(
                    name=name,
                    hash=md5_hash,
                    contents=thrift_json,
                    confType=conf_type,
                    localPath=json_file
                )

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


def compute_and_upload_diffs(
        branch: str,
        zipline_hub: ZiplineHub,
        local_repo_confs: dict[tuple, Conf],
        format: Format = Format.TEXT,
) -> dict[str, Conf]:
    # Group by confType — within a single type, names are guaranteed unique.
    # local_repo_confs is keyed by (name, confType) to prevent same-name cross-type collisions.
    confs_by_type = defaultdict(dict)
    for _key, conf in local_repo_confs.items():
        confs_by_type[conf.confType][conf.name] = conf

    total_count = len(local_repo_confs)
    print_step(f"🧮 Computed hashes for {total_count} local files.", format=format)

    all_diffed_confs = {}
    names_to_hashes_by_type = {}
    server_supports_type_scoped = False

    for conf_type, type_confs in confs_by_type.items():
        conf_type_name = conf_type.name if hasattr(conf_type, "name") else str(conf_type)
        names_to_hashes = {name: c.hash for name, c in type_confs.items()}
        names_to_hashes_by_type[conf_type] = names_to_hashes

        diff_response = zipline_hub.call_diff_api(names_to_hashes, conf_type=conf_type_name)
        # Any truthy value from the server means it supports type-scoped uploads
        server_supports_type_scoped = server_supports_type_scoped or bool(
            diff_response.get("supportsTypeScopedUpload")
        )

        changed_conf_names: list[str] = diff_response["diff"]
        if changed_conf_names:
            diffed = {k: type_confs[k] for k in changed_conf_names}
            all_diffed_confs.update(diffed)

    if not server_supports_type_scoped:
        print_warning(
            "The Zipline Hub service does not support type-scoped uploads. "
            "Upgrade the orchestration service to enable it. "
            "Falling back to legacy sync — conf name collisions across types may occur.",
            format=format,
        )

    if not all_diffed_confs:
        print_success(
            f"Remote contains all local files. No need to upload '{branch}'.", format=format
        )
    else:
        unchanged = total_count - len(all_diffed_confs)
        print_info(
            f"🔍 Detected {len(all_diffed_confs)} changes on local branch '{branch}'. {unchanged} unchanged.",
            format=format,
        )

        conf_names_str = "\n    - ".join(all_diffed_confs.keys())
        format_print(f"    - {conf_names_str}", format=format)

        diff_confs = [conf.__dict__ for conf in all_diffed_confs.values()]
        zipline_hub.call_upload_api(branch=branch, diff_confs=diff_confs)
        print_step(
            f"⬆️ Uploaded {len(all_diffed_confs)} changed confs to branch '{branch}'.", format=format
        )

    if server_supports_type_scoped:
        # Sync per type so the scoped upsert only touches confs of that type
        for conf_type, names_to_hashes in names_to_hashes_by_type.items():
            conf_type_name = conf_type.name if hasattr(conf_type, "name") else str(conf_type)
            zipline_hub.call_sync_api(branch=branch, names_to_hashes=names_to_hashes,
                                       conf_type=conf_type_name)
    else:
        # Legacy path: single sync call with all confs combined
        all_names_to_hashes = {n: h for m in names_to_hashes_by_type.values() for n, h in m.items()}
        zipline_hub.call_sync_api(branch=branch, names_to_hashes=all_names_to_hashes)

    print_success(f"{total_count} hashes updated on branch '{branch}'.", format=format)
    return all_diffed_confs
