import json
import os

import click
from attr import dataclass

from ai.chronon.cli.git_utils import get_current_branch
from ai.chronon.planner.ttypes import Mode
from ai.chronon.repo import hub_uploader, utils
from ai.chronon.repo.constants import RunMode
from ai.chronon.repo.zipline_hub import ZiplineHub


@click.group()
def hub():
    pass


#### Common click options
def common_options(func):
    func = click.option("--repo", help="Path to chronon repo", default=".")(func)
    func = click.option("--conf", required=True, help="Conf param - required for every mode")(func)
    return func

def ds_option(func):
    return click.option("--ds", help="the end partition to backfill the data")(func)

def start_ds_option(func):
    return click.option(
    "--start-ds",
    help="override the original start partition for a range backfill. "
    "It only supports staging query, group by backfill and join jobs. "
    "It could leave holes in your final output table due to the override date range.",)(func)


def end_ds_option(func):
    return click.option("--end-ds", help="the end ds for a range backfill")(func)

def force_recompute_option(func):
    return click.option("--force-recompute", is_flag=True, default=False,
                        help="Force recompute the backfill even if the data is already present in the output table.")(func)


def submit_workflow(repo,
                    conf,
                    mode,
                    start_ds,
                    end_ds,
                    force_recompute=False):

    hub_conf = get_hub_conf(conf)
    zipline_hub = ZiplineHub(base_url=hub_conf.hub_url)
    conf_name_to_hash_dict = hub_uploader.build_local_repo_hashmap(root_dir= repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_hash_dict)

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)


    response_json = zipline_hub.call_workflow_start_api(
        conf_name=conf_name,
        mode=mode,
        branch=branch,  # Get the current branch
        user=os.environ.get('USER'),
        start=start_ds,
        end=end_ds,
        conf_hash=conf_name_to_hash_dict[conf_name].hash,
        force_recompute=force_recompute
    )

    workflow_id = response_json.get("workflowId", "N/A")
    print(" 🆔 Workflow Id:", workflow_id)
    print_wf_url(
        conf=conf,
        conf_name=conf_name,
        mode=RunMode.BACKFILL.value,
        workflow_id=workflow_id
    )

def submit_schedule(repo,
                    conf
):

    hub_conf = get_hub_conf(conf)
    zipline_hub = ZiplineHub(base_url=hub_conf.hub_url)
    conf_name_to_obj_dict = hub_uploader.build_local_repo_hashmap(root_dir= repo)
    branch = get_current_branch()

    hub_uploader.compute_and_upload_diffs(branch, zipline_hub=zipline_hub, local_repo_confs=conf_name_to_obj_dict)

    # get conf name
    conf_name = utils.get_metadata_name_from_conf(repo, conf)
    schedule_modes = get_schedule_modes(conf)
    # create a dict for RunMode.BACKFILL.value and RunMode.DEPLOY.value to schedule_modes.offline_schedule and schedule_modes.online
    modes = {
        RunMode.BACKFILL.value.upper(): schedule_modes.offline_schedule,
        RunMode.DEPLOY.value.upper(): schedule_modes.online
    }

    response_json = zipline_hub.call_schedule_api(
        modes=modes,
        branch=branch,
        conf_name=conf_name,
        conf_hash=conf_name_to_obj_dict[conf_name].hash,
    )

    schedules = response_json.get("schedules", "N/A")
    readable_schedules = {Mode._VALUES_TO_NAMES[int(k)]: v for k,v in schedules.items()}
    print(" 🗓️ Schedules Deployed:", readable_schedules)


# zipline hub backfill --conf=compiled/joins/join
# adhoc backfills
@hub.command()
@common_options
@start_ds_option
@end_ds_option
@force_recompute_option
def backfill(repo,
             conf,
             start_ds,
             end_ds,
             force_recompute):
    """
    - Submit a backfill job to Zipline.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to backfill.
    """
    submit_workflow(repo, conf, RunMode.BACKFILL.value, start_ds, end_ds, force_recompute)



# zipline hub run-adhoc --conf=compiled/joins/join
# currently only supports one-off deploy node submission
@hub.command()
@common_options
@end_ds_option
def run_adhoc(repo,
           conf,
           end_ds):
    """
    - Submit a one-off deploy job to Zipline. This submits the various jobs to allow your conf to be tested online.
    Response should contain a list of confs that are different from what's on remote.
    - Call upload API to upload the conf contents for the list of confs that were different.
    - Call the actual run API with mode set to deploy
    """
    submit_workflow(repo, conf, RunMode.DEPLOY.value, end_ds, end_ds)

# zipline hub schedule --conf=compiled/joins/join
@hub.command()
@common_options
def schedule(repo, conf):
    """
    - Deploys a schedule for the specified conf to Zipline. This allows your conf to have various associated jobs run on a schedule.
    This verb will introspect your conf to determine which of its jobs need to be scheduled (or paused if turned off) based on the
    'offline_schedule' and 'online' fields.
    """
    submit_schedule(repo, conf)


def get_metadata_map(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    metadata_map = data['metaData']
    return metadata_map

def get_common_env_map(file_path):
    metadata_map = get_metadata_map(file_path)
    common_env_map = metadata_map['executionInfo']['env']['common']
    return common_env_map


@dataclass
class HubConfig:
    hub_url: str
    frontend_url: str

@dataclass
class ScheduleModes:
    online: str
    offline_schedule: str

def get_hub_conf(conf_path):
    common_env_map = get_common_env_map(conf_path)
    hub_url = common_env_map.get("HUB_URL", os.environ.get("HUB_URL"))
    frontend_url = common_env_map.get("FRONTEND_URL", os.environ.get("FRONTEND_URL"))
    return HubConfig(hub_url=hub_url, frontend_url=frontend_url)

def get_schedule_modes(conf_path):
    metadata_map = get_metadata_map(conf_path)
    online_value = metadata_map.get("online", False)
    online = "true" if bool(online_value) else "false"
    offline_schedule = metadata_map['executionInfo'].get('scheduleCron', None)

    # check if offline_schedule is null or 'None' or '@daily' else throw an error
    valid_schedules = {None, "None", "@daily"}
    if offline_schedule not in valid_schedules:
        raise ValueError(f"Unsupported offline_schedule: {offline_schedule}. Only null, 'None', or '@daily' are supported.")
    offline_schedule = offline_schedule or "None"
    return ScheduleModes(online=online, offline_schedule=offline_schedule)

def print_wf_url(conf, conf_name, mode, workflow_id):

    hub_conf = get_hub_conf(conf)
    frontend_url = hub_conf.frontend_url

    if "compiled/joins" in conf:
        hub_conf_type = "joins"
    elif "compiled/staging_queries" in conf:
        hub_conf_type = "stagingqueries"
    elif "compiled/group_by" in conf:
        hub_conf_type = "groupby"
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


