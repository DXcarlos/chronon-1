"""Zipline admin CLI commands for loading images into customer registries and verifying deployments."""

import json
import logging
import os
import tarfile
import tempfile

import click
from rich.console import Console
from rich.table import Table

from ai.chronon.repo.registry_client import (
    DOCKER_HUB_REGISTRY,
    RegistryClient,
    RegistryError,
)

logger = logging.getLogger(__name__)
console = Console()

VALID_CLOUDS = ("gcp", "aws", "azure")


def _sha256(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


@click.group(help="Administrative commands for loading Zipline images and verifying deployments.")
def admin():
    pass


@admin.command(help="Load Zipline images from Docker Hub (or an air-gap bundle) into a private registry.")
@click.option(
    "--token",
    envvar="ZIPLINE_DOCKER_TOKEN",
    default=None,
    help="Docker Hub OAT / PAT. Can also be set via ZIPLINE_DOCKER_TOKEN env var.",
)
@click.option("--version", "version", required=True, help="Zipline version to load (e.g. 0.1.42).")
@click.option(
    "--cloud",
    required=True,
    type=click.Choice(VALID_CLOUDS, case_sensitive=False),
    help="Cloud provider variant.",
)
@click.option("--registry", required=True, help="Target private registry URL (e.g. us-docker.pkg.dev/project/repo).")
@click.option(
    "--artifact-store",
    default=None,
    help="Target blob store for engine JARs (e.g. gs://bucket/zipline or s3://bucket/zipline).",
)
@click.option(
    "--bundle",
    default=None,
    type=click.Path(exists=True),
    help="Path to air-gap tarball (alternative to --token for disconnected environments).",
)
@click.option("--docker-username", envvar="ZIPLINE_DOCKER_USERNAME", default="ziplineai", help="Docker Hub username.")
def load(token, version, cloud, registry, artifact_store, bundle, docker_username):
    """Load Zipline images into a private container registry."""
    if not token and not bundle:
        raise click.UsageError("Either --token (or ZIPLINE_DOCKER_TOKEN) or --bundle is required.")

    client = RegistryClient()

    if bundle:
        results = _load_from_bundle(client, bundle, version, cloud, registry)
    else:
        results = _load_from_docker_hub(client, token, docker_username, version, cloud, registry)

    if artifact_store:
        jar_results = _upload_engine_jars_to_store(client, registry, version, cloud, artifact_store)
        results.extend(jar_results)

    _print_summary(results, version, cloud, registry)


def _load_from_docker_hub(client, token, username, version, cloud, target_registry):
    """Pull images from Docker Hub and push to target registry."""
    client.authenticate(DOCKER_HUB_REGISTRY, username=username, password=token)
    _authenticate_target_registry(client, target_registry)

    results = []

    for image_type, src_repo, dst_repo in [
        ("managed", f"ziplineai/managed-{cloud}", f"ziplineai/managed-{cloud}"),
        ("engine", f"ziplineai/engine-{cloud}", f"ziplineai/engine-{cloud}"),
    ]:
        console.print(f"[bold]Copying {src_repo}:{version}...[/bold]")
        try:
            digest = client.copy_image(DOCKER_HUB_REGISTRY, src_repo, target_registry, dst_repo, version)
            results.append((image_type, f"{target_registry}/{dst_repo}:{version}", digest, "ok"))
        except RegistryError as e:
            results.append((image_type, f"{target_registry}/{dst_repo}:{version}", "", f"FAILED: {e}"))

    return results


def _load_from_bundle(client, bundle_path, version, cloud, target_registry):
    """Load images from an air-gap tarball and push to target registry."""
    _authenticate_target_registry(client, target_registry)

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        console.print(f"[bold]Extracting bundle {bundle_path}...[/bold]")
        with tarfile.open(bundle_path, "r:gz") as tar:
            tar.extractall(tmpdir)

        for image_name in [f"managed-{cloud}", f"engine-{cloud}"]:
            archive_path = os.path.join(tmpdir, f"{image_name}.tar")
            if not os.path.exists(archive_path):
                results.append((image_name, "", "", f"FAILED: {archive_path} not found in bundle"))
                continue

            dst_repo = f"ziplineai/{image_name}"
            console.print(f"[bold]Pushing {image_name}:{version} from bundle...[/bold]")
            try:
                digest = _push_oci_archive(client, archive_path, target_registry, dst_repo, version)
                results.append((image_name, f"{target_registry}/{dst_repo}:{version}", digest, "ok"))
            except RegistryError as e:
                results.append((image_name, f"{target_registry}/{dst_repo}:{version}", "", f"FAILED: {e}"))

    return results


def _push_oci_archive(client, archive_path, registry, repo, tag):
    """Push a `docker save` OCI archive to a registry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(archive_path, "r") as tar:
            tar.extractall(tmpdir)

        manifest_path = os.path.join(tmpdir, "manifest.json")
        with open(manifest_path) as f:
            archive_manifests = json.load(f)

        if not archive_manifests:
            raise RegistryError(f"No manifests found in {archive_path}")

        entry = archive_manifests[0]

        # Push config blob
        config_file = os.path.join(tmpdir, entry["Config"])
        with open(config_file, "rb") as f:
            config_data = f.read()
        config_digest = "sha256:" + _sha256(config_data)
        if not client.blob_exists(registry, repo, config_digest):
            client.push_blob(registry, repo, config_digest, config_data)

        # Push layer blobs
        layer_digests = []
        layer_sizes = []
        for layer_file_rel in entry["Layers"]:
            layer_file = os.path.join(tmpdir, layer_file_rel)
            with open(layer_file, "rb") as f:
                layer_data = f.read()
            layer_digest = "sha256:" + _sha256(layer_data)
            layer_size = len(layer_data)
            if not client.blob_exists(registry, repo, layer_digest):
                client.push_blob(registry, repo, layer_digest, layer_data)
            layer_digests.append(layer_digest)
            layer_sizes.append(layer_size)

        # Build and push manifest
        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "digest": config_digest,
                "size": len(config_data),
            },
            "layers": [
                {
                    "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "digest": d,
                    "size": s,
                }
                for d, s in zip(layer_digests, layer_sizes)
            ],
        }
        manifest_bytes = json.dumps(manifest, indent=2).encode()
        return client.put_manifest(
            registry, repo, tag, manifest_bytes, "application/vnd.docker.distribution.manifest.v2+json"
        )


def _upload_engine_jars_to_store(client, registry, version, cloud, artifact_store):
    """Extract JARs from the engine image and upload to a blob store."""
    results = []
    engine_repo = f"ziplineai/engine-{cloud}"

    console.print(f"[bold]Extracting engine JARs from {engine_repo}:{version}...[/bold]")

    try:
        manifest_bytes, _, _ = client.get_manifest(registry, engine_repo, version)
        manifest = json.loads(manifest_bytes)
    except RegistryError as e:
        results.append(("engine-jars", artifact_store, "", f"FAILED: {e}"))
        return results

    with tempfile.TemporaryDirectory() as tmpdir:
        for layer in manifest.get("layers", []):
            layer_digest = layer["digest"]
            layer_path = os.path.join(tmpdir, "layer.tar")
            client.extract_blob_to_file(registry, engine_repo, layer_digest, layer_path)

            try:
                with tarfile.open(layer_path, "r") as tar:
                    for member in tar.getmembers():
                        if member.name.endswith(".jar") or member.name.endswith(".json"):
                            tar.extract(member, tmpdir)
            except tarfile.ReadError:
                continue

        jars_dir = os.path.join(tmpdir, "jars")
        if os.path.isdir(jars_dir):
            for jar_file in os.listdir(jars_dir):
                local_path = os.path.join(jars_dir, jar_file)
                remote_path = f"{artifact_store.rstrip('/')}/release/{version}/jars/{jar_file}"
                try:
                    _upload_to_blob_store(local_path, remote_path)
                    results.append(("jar", remote_path, "", "ok"))
                except Exception as e:
                    results.append(("jar", remote_path, "", f"FAILED: {e}"))

    return results


def _upload_to_blob_store(local_path, remote_path):
    """Upload a file to GCS, S3, or Azure Blob Storage based on the URI scheme."""
    if remote_path.startswith("gs://"):
        _upload_to_gcs(local_path, remote_path)
    elif remote_path.startswith("s3://"):
        _upload_to_s3(local_path, remote_path)
    elif remote_path.startswith("https://") and ".blob.core.windows.net" in remote_path:
        _upload_to_azure_blob(local_path, remote_path)
    else:
        raise ValueError(f"Unsupported blob store scheme: {remote_path}")


def _upload_to_gcs(local_path, gcs_path):
    from google.cloud import storage as gcs_storage

    parts = gcs_path[5:].split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1] if len(parts) > 1 else ""

    gcs_client = gcs_storage.Client()
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    logger.info(f"Uploaded {local_path} -> {gcs_path}")


def _upload_to_s3(local_path, s3_path):
    import boto3

    parts = s3_path[5:].split("/", 1)
    bucket_name = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket_name, key)
    logger.info(f"Uploaded {local_path} -> {s3_path}")


def _upload_to_azure_blob(local_path, azure_path):
    from urllib.parse import urlparse

    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient

    credential = DefaultAzureCredential()
    parsed = urlparse(azure_path)
    account_url = f"{parsed.scheme}://{parsed.netloc}"
    path_parts = parsed.path.lstrip("/").split("/", 1)
    container = path_parts[0]
    blob_name = path_parts[1] if len(path_parts) > 1 else ""

    blob_service = BlobServiceClient(account_url=account_url, credential=credential)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    logger.info(f"Uploaded {local_path} -> {azure_path}")


def _authenticate_target_registry(client, registry):
    """Set up auth for the target registry using ambient cloud credentials."""
    if "pkg.dev" in registry:
        try:
            import subprocess

            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True
            )
            client.authenticate(registry, username="oauth2accesstoken", password=result.stdout.strip())
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print("[yellow]Warning: Could not get gcloud access token. Target registry auth may fail.[/yellow]")
    elif ".dkr.ecr." in registry:
        try:
            import base64

            import boto3

            ecr = boto3.client("ecr")
            token_resp = ecr.get_authorization_token()
            auth_data = token_resp["authorizationData"][0]
            decoded = base64.b64decode(auth_data["authorizationToken"]).decode()
            username, password = decoded.split(":", 1)
            client.authenticate(registry, username=username, password=password)
        except Exception:
            console.print("[yellow]Warning: Could not get ECR auth token. Target registry auth may fail.[/yellow]")
    elif ".azurecr.io" in registry:
        try:
            import subprocess

            result = subprocess.run(
                ["az", "acr", "login", "--name", registry.split(".")[0], "--expose-token"],
                capture_output=True,
                text=True,
                check=True,
            )
            token_data = json.loads(result.stdout)
            client.authenticate(
                registry, username="00000000-0000-0000-0000-000000000000", password=token_data["accessToken"]
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print("[yellow]Warning: Could not get ACR token. Target registry auth may fail.[/yellow]")


def _print_summary(results, version, cloud, registry):
    """Print a summary table of the load operation."""
    table = Table(title=f"Zipline {version} ({cloud}) -> {registry}")
    table.add_column("Type", style="cyan")
    table.add_column("Reference", style="white")
    table.add_column("Status", style="green")

    all_ok = True
    for entry_type, ref, _digest, status in results:
        style = "green" if status == "ok" else "red"
        table.add_row(entry_type, ref, f"[{style}]{status}[/{style}]")
        if status != "ok":
            all_ok = False

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All artifacts loaded successfully.[/bold green]")
        console.print("\nFor terraform.tfvars:")
        console.print(f'  managed_image = "{registry}/ziplineai/managed-{cloud}:{version}"')
        console.print(f'  engine_image  = "{registry}/ziplineai/engine-{cloud}:{version}"')
    else:
        console.print("\n[bold red]Some artifacts failed to load. See errors above.[/bold red]")
        raise SystemExit(1)


@admin.command(help="Verify a running Zipline deployment.")
@click.option("--hub-url", required=True, help="URL of the running Zipline hub (e.g. https://hub.example.com).")
@click.option("--expected-version", default=None, help="Expected Zipline version (optional).")
def verify(hub_url, expected_version):
    """Check that a Zipline hub is reachable and healthy."""
    import urllib3

    http = urllib3.PoolManager()
    hub_url = hub_url.rstrip("/")

    results = []

    # Check hub health via /debug
    console.print(f"[bold]Checking hub health at {hub_url}/debug...[/bold]")
    try:
        resp = http.request("GET", f"{hub_url}/debug", timeout=10.0)
        if resp.status == 200:
            body = json.loads(resp.data)
            actual_version = body.get("version", "unknown")
            results.append(("Hub Health", f"{hub_url}/debug", "ok", f"version={actual_version}"))

            if expected_version and actual_version != expected_version:
                results.append((
                    "Version Check", "", "WARN", f"Expected {expected_version}, got {actual_version}"
                ))
            elif expected_version:
                results.append(("Version Check", "", "ok", f"matches {expected_version}"))
        else:
            results.append(("Hub Health", f"{hub_url}/debug", "FAIL", f"HTTP {resp.status}"))
    except Exception as e:
        results.append(("Hub Health", f"{hub_url}/debug", "FAIL", str(e)))

    # Check upload API reachability
    console.print(f"[bold]Checking upload API at {hub_url}/upload/v2/diff...[/bold]")
    try:
        resp = http.request("POST", f"{hub_url}/upload/v2/diff", timeout=10.0, body=b"{}")
        if resp.status < 500:
            results.append(("Upload API", f"{hub_url}/upload/v2/diff", "ok", f"HTTP {resp.status} (reachable)"))
        else:
            results.append(("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", f"HTTP {resp.status}"))
    except Exception as e:
        results.append(("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", str(e)))

    # Print results
    table = Table(title=f"Zipline Deployment Verification: {hub_url}")
    table.add_column("Check", style="cyan")
    table.add_column("Endpoint", style="white")
    table.add_column("Status", style="green")
    table.add_column("Detail", style="white")

    all_ok = True
    for check, endpoint, status, detail in results:
        if status == "ok":
            style = "green"
        elif status == "WARN":
            style = "yellow"
        else:
            style = "red"
            all_ok = False
        table.add_row(check, endpoint, f"[{style}]{status}[/{style}]", detail)

    console.print(table)

    if all_ok:
        console.print("\n[bold green]All checks passed.[/bold green]")
    else:
        console.print("\n[bold red]Some checks failed.[/bold red]")
        raise SystemExit(1)
