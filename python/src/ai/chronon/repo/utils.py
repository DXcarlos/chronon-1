import json
import os
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from enum import Enum

from click import style

from ai.chronon.cli.compile.parse_teams import EnvOrConfigAttribute
from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import (
    APP_NAME_TEMPLATE,
    SCALA_VERSION_FOR_SPARK,
    SUPPORTED_SPARK,
)

LOG = get_logger()


class JobType(Enum):
    SPARK = "spark"
    FLINK = "flink"


def retry_decorator(retries=3, backoff=20):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            attempt = 0
            while attempt <= retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    LOG.exception(e)
                    sleep_time = attempt * backoff
                    LOG.info(
                        "[{}] Retry: {} out of {}/ Sleeping for {}".format(
                            func.__name__, attempt, retries, sleep_time
                        )
                    )
                    time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapped

    return wrapper


def get_environ_arg(env_name, ignoreError=False) -> str:
    value = os.environ.get(env_name)
    if not value and not ignoreError:
        raise ValueError(f"Please set {env_name} environment variable")
    return value


def get_default_warehouse_bucket() -> str:
    return f"zipline-warehouse-{get_customer_id()}"


def get_customer_id() -> str:
    return get_environ_arg("CUSTOMER_ID")


def extract_filename_from_path(path):
    return path.split("/")[-1]


def check_call(cmd):
    LOG.info("Running command: " + cmd)
    return subprocess.check_call(cmd.split(), bufsize=0)


def check_output(cmd):
    LOG.info("Running command: " + cmd)
    return subprocess.check_output(cmd.split(), bufsize=0).strip()


def custom_json(conf):
    """Extract the json stored in customJson for a conf."""
    if conf.get("metaData", {}).get("customJson"):
        return json.loads(conf["metaData"]["customJson"])
    return {}


def download_only_once(url, path, skip_download=False):
    if skip_download:
        LOG.info("Skipping download of " + path)
        return
    should_download = True
    path = path.strip()
    if os.path.exists(path):
        content_output = check_output("curl -sI " + url).decode("utf-8")
        content_length = re.search("(content-length:\\s)(\\d+)", content_output.lower())
        remote_size = int(content_length.group().split()[-1])
        local_size = int(check_output("wc -c " + path).split()[0])
        LOG.info(
            """Files sizes of {url} vs. {path}
    Remote size: {remote_size}
    Local size : {local_size}""".format(**locals())
        )
        if local_size == remote_size:
            LOG.info("Sizes match. Assuming it's already downloaded.")
            should_download = False
        if should_download:
            LOG.info("Different file from remote at local: " + path + ". Re-downloading..")
            check_call("curl {} -o {} --connect-timeout 10".format(url, path))
    else:
        LOG.info("No file at: " + path + ". Downloading..")
        check_call("curl {} -o {} --connect-timeout 10".format(url, path))


# NOTE: this is only for the open source chronon. For the internal zipline version, we have a different jar to download.
@retry_decorator(retries=3, backoff=50)
def download_jar(
    version,
    jar_type="uber",
    release_tag=None,
    spark_version="2.4.0",
    skip_download=False,
):
    assert spark_version in SUPPORTED_SPARK, (
        f"Received unsupported spark version {spark_version}. "
        f"Supported spark versions are {SUPPORTED_SPARK}"
    )
    scala_version = SCALA_VERSION_FOR_SPARK[spark_version]
    maven_url_prefix = os.environ.get("CHRONON_MAVEN_MIRROR_PREFIX", None)
    default_url_prefix = "https://s01.oss.sonatype.org/service/local/repositories/public/content"
    url_prefix = maven_url_prefix if maven_url_prefix else default_url_prefix
    base_url = "{}/ai/chronon/spark_{}_{}".format(url_prefix, jar_type, scala_version)
    LOG.info("Downloading jar from url: " + base_url)
    jar_path = os.environ.get("CHRONON_DRIVER_JAR", None)
    if jar_path is None:
        if version == "latest":
            version = None
        if version is None:
            metadata_content = check_output("curl -s {}/maven-metadata.xml".format(base_url))
            meta_tree = ET.fromstring(metadata_content)
            versions = [
                node.text
                for node in meta_tree.findall("./versioning/versions/")
                if re.search(
                    r"^\d+\.\d+\.\d+{}$".format(
                        r"\_{}\d*".format(release_tag) if release_tag else ""
                    ),
                    node.text,
                )
            ]
            version = versions[-1]
        jar_url = (
            "{base_url}/{version}/spark_{jar_type}_{scala_version}-{version}-assembly.jar".format(
                base_url=base_url,
                version=version,
                scala_version=scala_version,
                jar_type=jar_type,
            )
        )
        jar_path = os.path.join("/tmp", extract_filename_from_path(jar_url))
        download_only_once(jar_url, jar_path, skip_download)
    return jar_path


def get_teams_json_file_path(repo_path):
    return os.path.join(repo_path, "teams.json")


def get_teams_py_file_path(repo_path):
    return os.path.join(repo_path, "teams.py")


def set_runtime_env_v3(params, conf):
    effective_mode = params.get("mode")

    runtime_env = {"APP_NAME": params.get("app_name")}

    if params.get("repo") and conf and effective_mode:
        # get the conf file
        conf_path = os.path.join(params["repo"], conf)
        if os.path.isfile(conf_path):
            with open(conf_path, "r") as infile:
                conf_json = json.load(infile)
                metadata = (
                    conf_json.get("metaData", {}) or conf_json
                )  # user may just pass metadata as the entire json
                env = metadata.get("executionInfo", {}).get("env", {})
                runtime_env.update(
                    env.get(EnvOrConfigAttribute.ENV, {}).get(effective_mode, {})
                    or env.get("common", {})
                )
                # Also set APP_NAME
                try:
                    _, conf_type, team, _ = conf.split("/")[-4:]
                    if not team:
                        team = "default"
                    # context is the environment in which the job is running, which is provided from the args,
                    # default to be dev.
                    if params["env"]:
                        context = params["env"]
                    else:
                        context = "dev"
                    LOG.info(f"Context: {context} -- conf_type: {conf_type} -- team: {team}")

                    runtime_env["APP_NAME"] = APP_NAME_TEMPLATE.format(
                        mode=effective_mode,
                        conf_type=conf_type,
                        context=context,
                        name=conf_json["metaData"]["name"],
                    )
                except Exception:
                    LOG.warn(
                        "Failed to set APP_NAME due to invalid conf path: {}, please ensure to supply the "
                        "relative path to zipline/ folder".format(conf)
                    )
        else:
            if not params.get("app_name") and not os.environ.get("APP_NAME"):
                # Provide basic app_name when no conf is defined.
                # Modes like metadata-upload and metadata-export can rely on conf-type or folder rather than a conf.
                runtime_env["APP_NAME"] = "_".join(
                    [k for k in ["chronon", effective_mode.replace("-", "_")] if k is not None]
                )
    for key, value in runtime_env.items():
        if key not in os.environ and value is not None:
            LOG.info(f"Setting to environment: {key}={value}")
            print(f"Setting to environment: {key}={value}")
            os.environ[key] = value


# TODO: delete this when we cutover
def set_runtime_env(params):
    """
    Setting the runtime environment variables.
    These are extracted from the common env, the team env and the common env.
    In order to use the environment variables defined in the configs as overrides for the args in the cli this method
    needs to be run before the runner and jar downloads.

    The order of priority is:
        - Environment variables existing already.
        - Environment variables derived from args (like app_name)
        - conf.metaData.modeToEnvMap for the mode (set on config)
        - team's dev environment for each mode set on teams.json
        - team's prod environment for each mode set on teams.json
        - default team environment per context and mode set on teams.json
        - Common Environment set in teams.json
    """

    environment = {
        "common_env": {},
        "conf_env": {},
        "default_env": {},
        "team_env": {},
        "production_team_env": {},
        "cli_args": {},
    }

    conf_type = None
    # Normalize modes that are effectively replacement of each other (streaming/local-streaming/streaming-client)
    effective_mode = params["mode"]
    if effective_mode and "streaming" in effective_mode:
        effective_mode = "streaming"
    if params["repo"]:
        # Break if teams.json and teams.py exists
        teams_json_file = get_teams_json_file_path(params["repo"])
        teams_py_file = get_teams_py_file_path(params["repo"])

        if os.path.exists(teams_json_file) and os.path.exists(teams_py_file):
            raise ValueError("Both teams.json and teams.py exist. Please only use teams.py.")

        if os.path.exists(teams_json_file):
            set_runtime_env_teams_json(environment, params, effective_mode, teams_json_file)
        if params["app_name"]:
            environment["cli_args"]["APP_NAME"] = params["app_name"]
        else:
            if not params["app_name"] and not environment["cli_args"].get("APP_NAME"):
                # Provide basic app_name when no conf is defined.
                # Modes like metadata-upload and metadata-export can rely on conf-type or folder rather than a conf.
                environment["cli_args"]["APP_NAME"] = "_".join(
                    [
                        k
                        for k in [
                            "chronon",
                            conf_type,
                            (params["mode"].replace("-", "_") if params["mode"] else None),
                        ]
                        if k is not None
                    ]
                )

        # Adding these to make sure they are printed if provided by the environment.
        environment["cli_args"]["CHRONON_DRIVER_JAR"] = params["chronon_jar"]
        environment["cli_args"]["CHRONON_ONLINE_JAR"] = params["online_jar"]
        environment["cli_args"]["CHRONON_ONLINE_CLASS"] = params["online_class"]
        order = [
            "conf_env",
            "team_env",  # todo: team_env maybe should be below default/common_env
            "production_team_env",
            "default_env",
            "common_env",
            "cli_args",
        ]
        LOG.info("Setting env variables:")
        for key in os.environ:
            if any([key in (environment.get(set_key, {}) or {}) for set_key in order]):
                LOG.info(f"From <environment> found {key}={os.environ[key]}")
        for set_key in order:
            for key, value in (environment.get(set_key, {}) or {}).items():
                if key not in os.environ and value is not None:
                    LOG.info(f"From <{set_key}> setting {key}={value}")
                    os.environ[key] = value


# TODO: delete this when we cutover
def set_runtime_env_teams_json(environment, params, effective_mode, teams_json_file):
    if os.path.exists(teams_json_file):
        with open(teams_json_file, "r") as infile:
            teams_json = json.load(infile)
        # we should have a fallback if user wants to set to something else `default`
        environment["common_env"] = teams_json.get("default", {}).get("common_env", {})
        if params["conf"] and effective_mode:
            try:
                _, conf_type, team, _ = params["conf"].split("/")[-4:]
            except Exception as e:
                LOG.error(
                    "Invalid conf path: {}, please ensure to supply the relative path to zipline/ folder".format(
                        params["conf"]
                    )
                )
                raise e
            if not team:
                team = "default"
            # context is the environment in which the job is running, which is provided from the args,
            # default to be dev.
            if params["env"]:
                context = params["env"]
            else:
                context = "dev"
            LOG.info(f"Context: {context} -- conf_type: {conf_type} -- team: {team}")
            conf_path = os.path.join(params["repo"], params["conf"])
            if os.path.isfile(conf_path):
                with open(conf_path, "r") as conf_file:
                    conf_json = json.load(conf_file)

                    new_env = (
                        conf_json.get("metaData")
                        .get("executionInfo", {})
                        .get("env", {})
                        .get(effective_mode, {})
                    )

                    old_env = (
                        conf_json.get("metaData").get("modeToEnvMap", {}).get(effective_mode, {})
                    )

                    environment["conf_env"] = new_env if new_env else old_env

                    # Load additional args used on backfill.
                    if custom_json(conf_json) and effective_mode in [
                        "backfill",
                        "backfill-left",
                        "backfill-final",
                    ]:
                        environment["conf_env"]["CHRONON_CONFIG_ADDITIONAL_ARGS"] = " ".join(
                            custom_json(conf_json).get("additional_args", [])
                        )
                    environment["cli_args"]["APP_NAME"] = APP_NAME_TEMPLATE.format(
                        mode=effective_mode,
                        conf_type=conf_type,
                        context=context,
                        name=conf_json["metaData"]["name"],
                    )
                environment["team_env"] = teams_json[team].get(context, {}).get(effective_mode, {})
                # fall-back to prod env even in dev mode when dev env is undefined.
                environment["production_team_env"] = (
                    teams_json[team].get("production", {}).get(effective_mode, {})
                )
                # By default use production env.
                environment["default_env"] = (
                    teams_json.get("default", {}).get("production", {}).get(effective_mode, {})
                )
                environment["cli_args"]["CHRONON_CONF_PATH"] = conf_path
    if params["app_name"]:
        environment["cli_args"]["APP_NAME"] = params["app_name"]
    else:
        if not params["app_name"] and not environment["cli_args"].get("APP_NAME"):
            # Provide basic app_name when no conf is defined.
            # Modes like metadata-upload and metadata-export can rely on conf-type or folder rather than a conf.
            environment["cli_args"]["APP_NAME"] = "_".join(
                [
                    k
                    for k in [
                        "chronon",
                        conf_type,
                        params["mode"].replace("-", "_") if params["mode"] else None,
                    ]
                    if k is not None
                ]
            )

    # Adding these to make sure they are printed if provided by the environment.
    environment["cli_args"]["CHRONON_DRIVER_JAR"] = params["chronon_jar"]
    environment["cli_args"]["CHRONON_ONLINE_JAR"] = params["online_jar"]
    environment["cli_args"]["CHRONON_ONLINE_CLASS"] = params["online_class"]
    order = [
        "conf_env",
        "team_env",
        "production_team_env",
        "default_env",
        "common_env",
        "cli_args",
    ]
    LOG.info("Setting env variables:")
    for key in os.environ:
        if any([key in environment[set_key] for set_key in order]):
            LOG.info(f"From <environment> found {key}={os.environ[key]}")
    for set_key in order:
        for key, value in environment[set_key].items():
            if key not in os.environ and value is not None:
                LOG.info(f"From <{set_key}> setting {key}={value}")
                os.environ[key] = value


def split_date_range(start_date, end_date, parallelism):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    if start_date > end_date:
        raise ValueError("Start date should be earlier than end date")
    total_days = (end_date - start_date).days + 1  # +1 to include the end_date in the range

    # Check if parallelism is greater than total_days
    if parallelism > total_days:
        raise ValueError("Parallelism should be less than or equal to total days")

    split_size = total_days // parallelism
    date_ranges = []

    for i in range(parallelism):
        split_start = start_date + timedelta(days=i * split_size)
        if i == parallelism - 1:
            split_end = end_date
        else:
            split_end = split_start + timedelta(days=split_size - 1)
        date_ranges.append((split_start.strftime("%Y-%m-%d"), split_end.strftime("%Y-%m-%d")))
    return date_ranges


def get_metadata_name_from_conf(repo_path, conf_path):
    with open(os.path.join(repo_path, conf_path), "r") as conf_file:
        data = json.load(conf_file)
        return data.get("metaData", {}).get("name", None)


def print_possible_confs(conf, repo, *args, **kwargs):
    conf_location = os.path.join(repo, conf)
    conf_dirname = os.path.dirname(conf_location)
    if os.path.exists(conf_dirname):
        print(
            f"Possible confs from {style(conf_dirname, fg='yellow')}: \n -",
            "\n - ".join([name for name in os.listdir(conf_dirname)]),
        )
    else:
        print(f"Directory does not exist: {style(conf_dirname, fg='yellow')}")
