import json
import multiprocessing
import os
from typing import List

import boto3

from ai.chronon.logger import get_logger
from ai.chronon.repo.constants import ROUTES, ZIPLINE_DIRECTORY
from ai.chronon.repo.default_runner import Runner
from ai.chronon.repo.utils import (
    JobType,
    check_call,
    extract_filename_from_path,
    get_customer_id,
    split_date_range,
)

LOG = get_logger()

# AWS SPECIFIC CONSTANTS
EMR_ENTRY = "ai.chronon.integrations.aws.EmrSubmitter"
ZIPLINE_AWS_JAR_DEFAULT = "cloud_aws_lib_deploy.jar"
ZIPLINE_AWS_ONLINE_CLASS_DEFAULT = "ai.chronon.integrations.aws.AwsApiImpl"
ZIPLINE_AWS_FLINK_JAR_DEFAULT = "flink_assembly_deploy.jar"
ZIPLINE_AWS_SERVICE_JAR = "service_assembly_deploy.jar"

LOCAL_FILE_TO_ETAG_JSON = f"{ZIPLINE_DIRECTORY}/local_file_to_etag.json"

EMR_MOUNT_FILE_PREFIX = "/mnt/zipline/"


class AwsRunner(Runner):
    def __init__(self, args):
        aws_jar_path = AwsRunner.download_zipline_aws_jar(
            ZIPLINE_DIRECTORY, get_customer_id(), args["version"], ZIPLINE_AWS_JAR_DEFAULT
        )
        service_jar_path = AwsRunner.download_zipline_aws_jar(
            ZIPLINE_DIRECTORY, get_customer_id(), args["version"], ZIPLINE_AWS_SERVICE_JAR
        )
        jar_path = f"{service_jar_path}:{aws_jar_path}" if args["mode"] == "fetch" else aws_jar_path
        self.version = args.get("version", "latest")

        super().__init__(args, os.path.expanduser(jar_path))

    @staticmethod
    def upload_s3_file(bucket_name: str, source_file_name: str, destination_blob_name: str):
        """Uploads a file to the bucket."""
        obj = boto3.client("s3")
        try:
            obj.upload_file(source_file_name, bucket_name, destination_blob_name)
            print(
                f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}."
            )
            return f"s3://{bucket_name}/{destination_blob_name}"
        except Exception as e:
            raise RuntimeError(f"Failed to upload {source_file_name}: {str(e)}") from e

    @staticmethod
    def download_zipline_aws_jar(
        destination_dir: str, customer_id: str, version: str, jar_name: str
    ):
        s3_client = boto3.client("s3")
        destination_path = f"{destination_dir}/{jar_name}"
        source_key_name = f"release/{version}/jars/{jar_name}"
        bucket_name = f"zipline-artifacts-{customer_id}"

        are_identical = (
            AwsRunner.compare_s3_and_local_file_hashes(
                bucket_name, source_key_name, destination_path
            )
            if os.path.exists(destination_path)
            else False
        )

        if are_identical:
            print(f"{destination_path} matches S3 {bucket_name}/{source_key_name}")
        else:
            print(f"{destination_path} does NOT match S3 {bucket_name}/{source_key_name}")
            print(f"Downloading {jar_name} from S3...")

            s3_client.download_file(
                Filename=destination_path, Bucket=bucket_name, Key=source_key_name
            )
            # Persist ETag to prevent downloading the same file next time
            etag = AwsRunner.get_s3_file_hash(bucket_name, source_key_name)
            if os.path.exists(LOCAL_FILE_TO_ETAG_JSON):
                with open(LOCAL_FILE_TO_ETAG_JSON, "r") as file:
                    data = json.load(file)

                # Add the new entry
                data[destination_path] = etag

                # Write the updated dictionary back to the file
                with open(LOCAL_FILE_TO_ETAG_JSON, "w") as file:
                    json.dump(data, file)
            else:
                with open(LOCAL_FILE_TO_ETAG_JSON, "w") as file:
                    data = {destination_path: etag}
                    json.dump(data, file)

        return destination_path

    @staticmethod
    def get_s3_file_hash(bucket_name: str, file_name: str):
        s3_client = boto3.client("s3")
        response = s3_client.head_object(Bucket=bucket_name, Key=file_name)
        return response["ETag"].strip('"')

    @staticmethod
    def get_local_file_hash(file_name: str):
        # read in the json file
        if os.path.exists(LOCAL_FILE_TO_ETAG_JSON):
            with open(LOCAL_FILE_TO_ETAG_JSON, "r") as f:
                data = json.load(f)
                if file_name in data:
                    return data[file_name]
        return None

    @staticmethod
    def compare_s3_and_local_file_hashes(bucket_name: str, s3_file_path: str, local_file_path: str):
        try:
            s3_hash = AwsRunner.get_s3_file_hash(bucket_name, s3_file_path)
            local_hash = AwsRunner.get_local_file_hash(local_file_path)
            print(f"Local hash: {local_hash}, S3 hash: {s3_hash}")
            return s3_hash == local_hash
        except Exception as e:
            print(f"Error comparing files: {str(e)}")
            return False

    def generate_emr_submitter_args(
        self,
        user_args: str,
        job_type: JobType = JobType.SPARK,
        local_files_to_upload: List[str] = None,
    ):
        customer_warehouse_bucket_name = f"zipline-warehouse-{get_customer_id()}"
        s3_files = []
        for source_file in local_files_to_upload:
            # upload to `metadata` folder
            destination_file_path = f"metadata/{extract_filename_from_path(source_file)}"
            s3_files.append(
                AwsRunner.upload_s3_file(
                    customer_warehouse_bucket_name, source_file, destination_file_path
                )
            )

        # we also want the additional-confs included here. it should already be in the bucket

        zipline_artifacts_bucket_prefix = "s3://zipline-artifacts"

        s3_files.append(
            f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}/confs/additional-confs.yaml"
        )

        s3_file_args = ",".join(s3_files)

        # include jar uri. should also already be in the bucket
        jar_uri = (
            f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}"
            + f"/release/{self.version}/jars/{ZIPLINE_AWS_JAR_DEFAULT}"
        )

        final_args = (
            "{user_args} --jar-uri={jar_uri} --job-type={job_type} --main-class={main_class}"
        )

        if job_type == JobType.FLINK:
            main_class = "ai.chronon.flink.FlinkJob"
            flink_jar_uri = (
                f"{zipline_artifacts_bucket_prefix}-{get_customer_id()}"
                + f"/jars/{ZIPLINE_AWS_FLINK_JAR_DEFAULT}"
            )
            return (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                )
                + f" --flink-main-jar-uri={flink_jar_uri}"
            )

        elif job_type == JobType.SPARK:
            main_class = "ai.chronon.spark.Driver"
            return (
                final_args.format(
                    user_args=user_args,
                    jar_uri=jar_uri,
                    job_type=job_type.value,
                    main_class=main_class,
                )
                + f" --additional-conf-path={EMR_MOUNT_FILE_PREFIX}additional-confs.yaml"
                f" --files={s3_file_args}"
            )
        else:
            raise ValueError(f"Invalid job type: {job_type}")

    def run(self):
        command_list = []
        if self.mode == "info":
            command_list.append(
                "python3 {script} --conf {conf} --ds {ds} --repo {repo}".format(
                    script=self.render_info, conf=self.conf, ds=self.ds, repo=self.repo
                )
            )
        elif self.sub_help or self.mode == "fetch":
            entrypoint = "ai.chronon.online.fetcher.FetcherMain"
            command_list.append(
                "java -cp {jar} {entrypoint} {subcommand} {args}".format(
                    jar=self.jar_path,
                    entrypoint=entrypoint,
                    args="--help" if self.sub_help else self._gen_final_args(),
                    subcommand=ROUTES[self.conf_type][self.mode],
                )
            )
        elif self.mode in ["streaming", "streaming-client"]:
            raise ValueError("Streaming is not supported for AWS yet.")
        else:
            local_files_to_upload_to_aws = []
            if self.conf:
                local_files_to_upload_to_aws.append(os.path.join(self.repo, self.conf))
            if self.parallelism > 1:
                assert self.start_ds is not None and self.ds is not None, (
                    "To use parallelism, please specify --start-ds and --end-ds to "
                    "break down into multiple backfill jobs"
                )
                date_ranges = split_date_range(self.start_ds, self.ds, self.parallelism)
                for start_ds, end_ds in date_ranges:
                    user_args = "{subcommand} {args} {additional_args}".format(
                        subcommand=ROUTES[self.conf_type][self.mode],
                        args=self._gen_final_args(
                            start_ds=start_ds,
                            end_ds=end_ds,
                            # when we download files from s3 to emr, they'll be mounted at /mnt/zipline
                            override_conf_path=(
                                EMR_MOUNT_FILE_PREFIX + extract_filename_from_path(self.conf)
                                if self.conf
                                else None
                            ),
                        ),
                        additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                    )

                    emr_args = self.generate_emr_submitter_args(
                        local_files_to_upload=local_files_to_upload_to_aws,
                        # for now, self.conf is the only local file that requires uploading to gcs
                        user_args=user_args,
                    )
                    command = f"java -cp {self.jar_path} {EMR_ENTRY} {emr_args}"
                    command_list.append(command)
            else:
                user_args = ("{subcommand} {args} {additional_args}").format(
                    subcommand=ROUTES[self.conf_type][self.mode],
                    args=self._gen_final_args(
                        start_ds=self.start_ds,
                        # when we download files from s3 to emr, they'll be mounted at /mnt/zipline
                        override_conf_path=(
                            EMR_MOUNT_FILE_PREFIX + extract_filename_from_path(self.conf)
                            if self.conf
                            else None
                        ),
                    ),
                    additional_args=os.environ.get("CHRONON_CONFIG_ADDITIONAL_ARGS", ""),
                )

                emr_args = self.generate_emr_submitter_args(
                    # for now, self.conf is the only local file that requires uploading
                    local_files_to_upload=local_files_to_upload_to_aws,
                    user_args=user_args,
                )
                command = f"java -cp {self.jar_path} {EMR_ENTRY} {emr_args}"
                command_list.append(command)

        if len(command_list) > 1:
            # parallel backfill mode
            with multiprocessing.Pool(processes=int(self.parallelism)) as pool:
                LOG.info(
                    "Running args list {} with pool size {}".format(command_list, self.parallelism)
                )
                pool.map(check_call, command_list)
        elif len(command_list) == 1:
            # TODO: add log tailing
            check_call(command_list[0])
