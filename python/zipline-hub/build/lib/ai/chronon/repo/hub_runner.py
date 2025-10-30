import json
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import click
from gen_thrift.planner.ttypes import Mode

from ai.chronon.cli.git_utils import get_current_branch
from ai.chronon.click_helpers import handle_compile, handle_conf_not_found
from ai.chronon.repo import hub_uploader, utils
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.utils import print_possible_confs
from ai.chronon.repo.zipline_hub import ZiplineHub

ALLOWED_DATE_FORMATS = ["%Y-%m-%d"]

DEFAULT_TEAM_METADATA_CONF = "compiled/teams_metadata/default/default_team_metadata"

@dataclass
class HubConfig:
    hub_url: str
    frontend_url: str
    sa_name: Optional[str] = None
    eval_url: Optional[str] = None


@dataclass
class ScheduleModes:
    online: str
    offline_schedule: str

def repo_option(func):
    return click.option("--repo", help="Path to chronon repo", default=".")(func)
def use_auth_option(func):
    return click.option(
        "--use-auth/--no-use-auth", help="Use authentication when connecting to Zipline Hub", default=True
    )(func)
def hub_url_option(func):
    return click.option(
        "--hub_url", help="Zipline Hub address, e.g. http://localhost:3903", default=None
    )(func)

#### Common click options
def common_options(func):
    func = repo_option(func)
    func = click.option("--conf", required=True, help="Conf param - required for every mode")(func)
    func = hub_url_option(func)
    func = use_auth_option(func)
    return func


def ds_option(func):
    return click.option(
        "--ds",
        help="the end partition to backfill the data",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
    )(func)


def start_ds_option(func):
    return click.option(
        "--start-ds",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
        help="override the original start partition for a range backfill. "
        "It only supports staging query, group by backfill and join jobs. "
        "It could leave holes in your final output table due to the override date range.",
    )(func)

def workflow_id_option(func):
    return click.option(
        "--workflow-id",
        help="Zipline workflow id",
        type=str,
        required=True,
    )(func)


def end_ds_option(func):
    return click.option(
        "--end-ds",
        help="the end ds for a range backfill",
        type=click.DateTime(formats=ALLOWED_DATE_FORMATS),
        default=str(date.today() - timedelta(days=2)),
    )(func)

def _get_zipline_hub(hub_url: Optional[str], hub_conf: Optional[HubConfig], use_auth: bool):
    if hub_url is not None:
        zipline_hub = ZiplineHub(base_url=hub_url, sa_name=hub_conf.sa_name, use_auth=use_auth)
    else:
        zipline_hub = ZiplineHub(base_url=hub_conf.hub_url, sa_name=hub_conf.sa_name, use_auth=use_auth)
    return zipline_hub

def submit_workflow(repo, conf, mode, start_ds, end_ds, hub_url=None, use_auth=True):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)

    response_json = zipline_hub.call_workflow_start_api(
        conf_name=conf_name,
        mode=mode,
        branch=branch,  # Get the current branch
        user=os.environ.get("USER"),
        start=start_ds,
        end=end_ds,
        conf_hash=conf_name_to_hash_dict[conf_name].hash,
        skip_long_running=False,
    )

    workflow_id = response_json.get("workflowId", "N/A")
    print(" 🆔 Workflow Id:", workflow_id)
    print_wf_url(
        conf=conf,
        conf_name=conf_name,
        mode=mode,
        workflow_id=workflow_id,
        repo=repo
    )


def submit_schedule(repo, conf, hub_url=None, use_auth=True):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = _get_zipline_hub(hub_url, hub_conf, use_auth)
    conf_name_to_obj_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_obj_dict
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    schedule_modes = get_schedule_modes(os.path.join(repo, conf))
    # create a dict for RunMode.BACKFILL.value and RunMode.DEPLOY.value to schedule_modes.offline_schedule and schedule_modes.online
    modes = {
        RunMode.BACKFILL.value.upper(): schedule_modes.offline_schedule,
        RunMode.DEPLOY.value.upper(): schedule_modes.online,
    }

    response_json = zipline_hub.call_schedule_api(
        modes=modes,
        branch=branch,
        conf_name=conf_name,
        conf_hash=conf_name_to_obj_dict[conf_name].hash,
    )

    schedules = response_json.get("schedules", "N/A")
    readable_schedules = {Mode._VALUES_TO_NAMES[int(k)]: v for k, v in schedules.items()}
    print(" 🗓️ Schedules Deployed:", readable_schedules)


# zipline backfill --conf=compiled/joins/join
# adhoc backfills
@click.command()
@common_options
@start_ds_option
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
def backfill(repo, conf, hub_url, use_auth, start_ds, end_ds, skip_compile):
    """
    - Submit a backfill job to Zipline.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to backfill.
    """
    submit_workflow(
        repo, conf, RunMode.BACKFILL.value, start_ds, end_ds, hub_url=hub_url, use_auth=use_auth
    )


# zipline run-adhoc --conf=compiled/joins/join
# currently only supports one-off deploy node submission
@click.command()
@common_options
@end_ds_option
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
def run_adhoc(repo, conf, hub_url, use_auth, end_ds, skip_compile):
    """
    - Submit a one-off deploy job to Zipline. This submits the various jobs to allow your conf to be tested online.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to deploy
    """
    submit_workflow(repo, conf, RunMode.DEPLOY.value, end_ds, end_ds, hub_url=hub_url, use_auth=use_auth)


# zipline schedule --conf=compiled/joins/join
@click.command()
@common_options
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
def schedule(repo, conf, hub_url, use_auth, skip_compile):
    """
    - Deploys a schedule for the specified conf to Zipline. This allows your conf to have various associated jobs run on a schedule.
    This verb will introspect your conf to determine which of its jobs need to be scheduled (or paused if turned off) based on the
    'offline_schedule' and 'online' fields.
    """
    submit_schedule(repo, conf, hub_url=hub_url, use_auth=use_auth)

@click.command()
@repo_option
@hub_url_option
@use_auth_option
@workflow_id_option
def cancel(repo, hub_url, use_auth, workflow_id):
    zipline_hub = _get_zipline_hub(hub_url, get_hub_conf_from_metadata_conf(DEFAULT_TEAM_METADATA_CONF, root_dir=repo), use_auth)
    zipline_hub.call_cancel_api(workflow_id)
    print(f" 🟢 Workflow cancelled: {workflow_id}")

def load_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)
    return data

def get_metadata_map(file_path):
    data = load_json(file_path)
    metadata_map = data["metaData"]
    return metadata_map


def get_common_env_map(file_path, skip_metadata_extraction=False):
    metadata_map = get_metadata_map(file_path) if not skip_metadata_extraction else load_json(file_path)
    common_env_map = metadata_map["executionInfo"]["env"]["common"]
    return common_env_map

# zipline eval --conf=compiled/joins/join
# localSparkSession evaluation of conf
@click.command()
@common_options
@click.option(
    "--eval-url",
    help="Eval Server",
    type=str,
    default=None
)
@handle_conf_not_found(log_error=True, callback=print_possible_confs)
@handle_compile
def eval(repo, conf, hub_url, use_auth, eval_url, skip_compile):
    """
    - Submit a eval job to Zipline.
    Response should contain a list of validation checks that are executed in a sparkLocalSession with Metadata access.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual eval API.
    """
    hub_conf = get_hub_conf(conf, root_dir=repo)
    zipline_hub = ZiplineHub(base_url=hub_url or hub_conf.hub_url, sa_name=hub_conf.sa_name, use_auth=use_auth, eval_url=eval_url or hub_conf.eval_url)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir=repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(
        branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict
    )

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)

    response_json = zipline_hub.call_eval_api(
        conf_name=conf_name,
        conf_hash_map={conf.name: conf.hash for conf in conf_name_to_hash_dict.values()},
    )
    if response_json.get("success"):
        print(" 🟢 Eval job finished successfully")
        print(response_json.get("message"))
    else:
        print(" 🔴 Eval job failed")
        print(response_json.get("message"))
        sys.exit(1)


def get_hub_conf(conf_path, root_dir="."):
    """
    Get the hub configuration from the config file or environment variables.
    This method is used when the args are not provided.
    Priority is arg -> environment variable -> common env.
    """
    file_path = os.path.join(root_dir, conf_path)
    common_env_map = get_common_env_map(file_path)
    common_env_map.update(os.environ) # Override config with cli args
    hub_url = common_env_map.get("HUB_URL")
    frontend_url = common_env_map.get("FRONTEND_URL")
    sa_name = common_env_map.get("SA_NAME")
    eval_url = common_env_map.get("EVAL_URL")
    return HubConfig(hub_url=hub_url, frontend_url=frontend_url, sa_name=sa_name, eval_url=eval_url)

def get_hub_conf_from_metadata_conf(metadata_path, root_dir="."):
    """
    Get the hub configuration from the config file or environment variables.
    This method is used when the args are not provided.
    Priority is arg -> environment variable -> common env.
    """
    file_path = os.path.join(root_dir, metadata_path)
    common_env_map = get_common_env_map(file_path, skip_metadata_extraction=True)
    common_env_map.update(os.environ) # Override config with cli args
    hub_url = common_env_map.get("HUB_URL")
    frontend_url = common_env_map.get("FRONTEND_URL")
    sa_name = common_env_map.get("SA_NAME")
    eval_url = common_env_map.get("EVAL_URL")
    return HubConfig(hub_url=hub_url, frontend_url=frontend_url, sa_name=sa_name, eval_url=eval_url)


def get_schedule_modes(conf_path):
    metadata_map = get_metadata_map(conf_path)
    online_value = metadata_map.get("online", False)
    online = "true" if bool(online_value) else "false"
    offline_schedule = metadata_map["executionInfo"].get("scheduleCron", None)

    # check if offline_schedule is null or 'None' or '@daily' else throw an error
    valid_schedules = {None, "None", "@daily"}
    if offline_schedule not in valid_schedules:
        raise ValueError(
            f"Unsupported offline_schedule: {offline_schedule}. Only null, 'None', or '@daily' are supported."
        )
    offline_schedule = offline_schedule or "None"
    return ScheduleModes(online=online, offline_schedule=offline_schedule)


def print_wf_url(conf, conf_name, mode, workflow_id, repo="."):
    hub_conf = get_hub_conf(conf, root_dir=repo)
    frontend_url = hub_conf.frontend_url

    if "compiled/joins" in conf:
        hub_conf_type = "joins"
    elif "compiled/staging_queries" in conf:
        hub_conf_type = "stagingqueries"
    elif "compiled/group_by" in conf:
        hub_conf_type = "groupbys"
    elif "compiled/models" in conf:
        hub_conf_type = "models"
    else:
        raise ValueError(f"Unsupported conf type: {conf}")

    def _mode_string():
        if mode == "backfill":
            return "offline"
        elif mode == "deploy":
            return "online"
        else:
            raise ValueError(f"Unsupported mode: {mode}")

    workflow_url = f"{frontend_url.rstrip('/')}/{hub_conf_type}/{conf_name}/{_mode_string()}?workflowId={workflow_id}"

    print(" 🔗 Workflow : " + workflow_url + "\n")

if __name__ == "__main__":
    hub()
