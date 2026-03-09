"""Zipline admin CLI commands for loading images into customer registries and verifying deployments."""

import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import traceback
from functools import partial

import click
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from ai.chronon.cli.theme import STYLE_ERROR, STYLE_SUCCESS, console
from ai.chronon.repo.constants import VALID_CLOUDS
from ai.chronon.repo.registry_client import (
    DOCKER_HUB_REGISTRY,
    ImageTarget,
    RegistryClient,
    RegistryError,
)
from ai.chronon.repo.utils import get_package_version, upload_to_blob_store

logger = logging.getLogger(__name__)


def _safe_extractall(tar, dest):
    """Extract tar members after validating no paths escape *dest* (path traversal guard)."""
    dest = os.path.realpath(dest)
    for member in tar.getmembers():
        member_path = os.path.realpath(os.path.join(dest, member.name))
        if not member_path.startswith(dest + os.sep) and member_path != dest:
            raise RuntimeError(f"Tar member {member.name!r} would escape destination directory")
    tar.extractall(dest)


_CLOUDS_WITH_EVAL = ("gcp", "azure")


def _app_images(cloud):
    """Return the list of (image_type, repo) tuples for application images (excludes engine)."""
    images = [
        ("hub", f"ziplineai/hub-{cloud}"),
        ("frontend", "ziplineai/web-ui"),
    ]
    if cloud in _CLOUDS_WITH_EVAL:
        images.insert(1, ("eval", f"ziplineai/eval-{cloud}"))
    return images


def _parse_registry(registry):
    """Parse a registry URL into (host, repo_prefix)."""
    from urllib.parse import urlparse

    parsed = urlparse(registry if "://" in registry else f"https://{registry}")
    host = parsed.hostname or ""
    prefix = parsed.path.strip("/")
    return host, prefix


# ── CLI commands ──────────────────────────────────────────────────────


@click.group(
    help="Administrative commands for initializing repos, loading images, and verifying deployments."
)
def admin():
    pass


@admin.command("install")
@click.argument("cloud", type=click.Choice(VALID_CLOUDS, case_sensitive=False))
@click.option(
    "--registry",
    default="local",
    show_default=True,
    help="Target registry URL (e.g. us-docker.pkg.dev/project/repo) or 'local' for the local Docker daemon.",
)
@click.option(
    "--api-token",
    envvar="ZIPLINE_API_TOKEN",
    default=None,
    help="Zipline API token for Docker Hub access. Can also be set via ZIPLINE_API_TOKEN env var. "
    "Optional when using --bundle or when already authenticated to Docker Hub.",
)
@click.option(
    "--release", default=None, help="Zipline release to load (e.g. 0.1.42). Defaults to the installed zipline-ai package version."
)
@click.option(
    "--artifact-prefix",
    default=None,
    help="Target prefix for engine JARs: a blob store URI (e.g. gs://bucket/zipline) or a local filesystem path.",
)
@click.option(
    "--bundle",
    default=None,
    type=click.Path(exists=True),
    help="Path to air-gap tarball (alternative to pulling from Docker Hub).",
)
def install(cloud, registry, api_token, release, artifact_prefix, bundle):
    """Install Zipline images into a private registry or the local Docker daemon.

    CLOUD is the cloud provider variant (gcp, aws, or azure).
    """
    for name in ("urllib3", "ai.chronon.logger"):
        logging.getLogger(name).setLevel(logging.WARNING)

    if release is None:
        release = get_package_version()
        if release == "unknown":
            release = "latest"
        console.print(f"Using release [bold]{release}[/bold]")
    else:
        pkg_version = get_package_version()
        # Normalize away any leading "v" before comparing since PyPI versions omit it.
        if pkg_version != "unknown" and release.lstrip("v") != pkg_version.lstrip("v"):
            if not click.confirm(
                f"Specified release {release} does not match installed zipline cli version {pkg_version}. Continue?"
            ):
                raise SystemExit(1)

    if registry == "local":
        _check_docker_available()
        target = ImageTarget()
    else:
        client = RegistryClient()
        host, prefix = _parse_registry(registry)
        target = ImageTarget(client, host, prefix)

    with _make_progress() as progress:
        if bundle:
            results = _load_from_bundle(target, bundle, release, cloud, progress)
        else:
            results = _load_from_docker_hub(target, api_token, release, cloud, progress)

        if artifact_prefix:
            if bundle:
                jar_results = _extract_engine_jars_from_bundle(
                    bundle, cloud, release, artifact_prefix, progress
                )
            else:
                hub_client = RegistryClient()
                _authenticate_docker_hub(hub_client, api_token)
                jar_results = _upload_engine_jars_to_store(
                    hub_client, DOCKER_HUB_REGISTRY, release, cloud, artifact_prefix, progress
                )
            results.extend(jar_results)

    _print_summary(results, release, cloud, registry)


# ── Progress helpers ──────────────────────────────────────────────────


def _make_progress():
    """Create a configured rich Progress bar."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    )


def _advance_progress(progress, task_id, n):
    """Callback for on_progress: advance a rich progress bar task by *n* bytes."""
    progress.update(task_id, advance=n)


def _update_status(progress, task_id, base_label, layer_sizes, line):
    """Callback for on_status: parse Docker pull/load output and advance progress.

    Docker non-TTY output emits lines like ``a2abf6c4d29d: Pull complete``.
    We advance the progress bar by one layer's worth of bytes per completed layer.
    """
    parts = line.split(": ", 1)
    if len(parts) == 2:
        _layer_id, status = parts
        if status in ("Pull complete", "Already exists"):
            if layer_sizes:
                progress.update(task_id, advance=layer_sizes.pop(0))
        elif status not in ("Waiting", "Pulling fs layer"):
            progress.update(task_id, description=f"{base_label}: {status[:40]}")
    elif "Digest:" in line or "Status:" in line:
        progress.update(task_id, description=base_label)


def _finish_task(progress, task_id, label, ok):
    """Remove a progress task and print a completion or failure line."""
    progress.remove_task(task_id)
    if ok:
        progress.console.print(f"[{STYLE_SUCCESS}] ✅ SUCCESS [/] {label}")
    else:
        progress.console.print(f"[{STYLE_ERROR}] 🔴 FAILED [/] {label}")


def _make_target_with_progress(target, progress, task_id, action_label, layer_sizes=None):
    """Create an ImageTarget copy wired to a progress bar task."""
    if target.is_local:
        return target.with_callbacks(
            on_status=partial(_update_status, progress, task_id, action_label, layer_sizes or []),
        )
    return target.with_callbacks(
        on_progress=partial(_advance_progress, progress, task_id),
    )


# ── Image loading ─────────────────────────────────────────────────────


def _load_from_docker_hub(target, api_token, release, cloud, progress):
    """Pull images from Docker Hub."""
    hub_client = None
    if target.is_local:
        if api_token:
            console.print("[bold]Logging in to Docker Hub...[/bold]")
            proc = subprocess.run(
                ["docker", "login", "-u", "ziplineai", "--password-stdin"],
                input=api_token,
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                console.print(
                    f"[yellow]Warning: docker login failed: {proc.stderr.strip()}[/yellow]"
                )
        # OCI client for querying image sizes (anonymous access suffices for public images)
        hub_client = RegistryClient()
        try:
            _authenticate_docker_hub(hub_client, api_token)
        except (click.UsageError, RegistryError):
            hub_client.authenticate(DOCKER_HUB_REGISTRY)
    else:
        _authenticate_docker_hub(target.client, api_token)
        _authenticate_target_registry(target.client, target.registry_host)

    results = []
    size_client = hub_client if target.is_local else target.client
    for image_type, repo in _app_images(cloud):
        action = "Pulling" if target.is_local else "Copying"
        label = f"{repo}:{release}"

        total_bytes, layer_sizes = None, []
        if size_client:
            try:
                if target.is_local:
                    total_bytes, layer_sizes = size_client.get_layer_sizes(
                        DOCKER_HUB_REGISTRY, repo, release
                    )
                else:
                    total_bytes = size_client.get_total_image_size(
                        DOCKER_HUB_REGISTRY, repo, release
                    )
            except RegistryError:
                pass

        task_id = progress.add_task(f"{action} {label}", total=total_bytes)
        img_target = _make_target_with_progress(
            target, progress, task_id, f"{action} {label}", layer_sizes
        )

        try:
            digest = img_target.copy_from_hub(repo, release)
            _finish_task(progress, task_id, label, ok=True)
            results.append((image_type, target.ref(repo, release), digest, "ok"))
        except RegistryError as e:
            _finish_task(progress, task_id, label, ok=False)
            console.print(f"[{STYLE_ERROR}]Error loading {label}:[/]\n{traceback.format_exc()}")
            results.append((image_type, target.ref(repo, release), "", f"FAILED: {e}"))
    return results


def _get_bundle_image_size(archive_path):
    """Return total bytes of all layer files in an OCI archive."""
    with tarfile.open(archive_path, "r") as tar:
        manifest_member = tar.getmember("manifest.json")
        with tar.extractfile(manifest_member) as f:
            archive_manifests = json.load(f)
        if not archive_manifests:
            return 0
        entry = archive_manifests[0]
        total = 0
        for layer_rel in entry.get("Layers", []):
            try:
                member = tar.getmember(layer_rel)
                total += member.size
            except KeyError:
                pass
        return total


def _load_from_bundle(target, bundle_path, release, cloud, progress):
    """Load images from an air-gap tarball."""
    if not target.is_local:
        _authenticate_target_registry(target.client, target.registry_host)

    results = []
    with tempfile.TemporaryDirectory() as tmpdir:
        console.print(f"[bold]Extracting bundle {bundle_path}...[/bold]")
        with tarfile.open(bundle_path, "r:gz") as tar:
            _safe_extractall(tar, tmpdir)

        for _image_type, repo in _app_images(cloud):
            image_name = repo.split("/")[-1]
            archive_path = os.path.join(tmpdir, f"{image_name}.tar")
            if not os.path.exists(archive_path):
                results.append(
                    (image_name, "", "", f"FAILED: {image_name}.tar not found in bundle")
                )
                continue

            action = "Pushing" if not target.is_local else "Loading"
            label = f"{image_name}:{release}"

            try:
                total_bytes = _get_bundle_image_size(archive_path)
            except Exception:
                total_bytes = None

            task_id = progress.add_task(f"{action} {label}", total=total_bytes)
            img_target = _make_target_with_progress(target, progress, task_id, f"{action} {label}")

            try:
                digest = img_target.load_archive(archive_path, repo, release)
                _finish_task(progress, task_id, label, ok=True)
                results.append((image_name, target.ref(repo, release), digest, "ok"))
            except RegistryError as e:
                _finish_task(progress, task_id, label, ok=False)
                console.print(f"[{STYLE_ERROR}]Error loading {label}:[/]\n{traceback.format_exc()}")
                results.append((image_name, target.ref(repo, release), "", f"FAILED: {e}"))
    return results


# ── Engine JAR extraction ─────────────────────────────────────────────


def _extract_jars_from_layer(layer_path, dest_dir):
    """Extract .jar and .json files from a single tar layer."""
    try:
        with tarfile.open(layer_path, "r") as tar:
            for member in tar.getmembers():
                if member.name.endswith(".jar") or member.name.endswith(".json"):
                    tar.extract(member, dest_dir)
    except tarfile.ReadError:
        pass


def _upload_jars_to_store(jars_dir, release, artifact_store):
    """Upload all files in jars_dir to the artifact store."""
    results = []
    if os.path.isdir(jars_dir):
        for jar_file in os.listdir(jars_dir):
            local_path = os.path.join(jars_dir, jar_file)
            remote_path = f"{artifact_store.rstrip('/')}/release/{release}/jars/{jar_file}"
            try:
                upload_to_blob_store(local_path, remote_path)
                results.append(("jar", remote_path, "", "ok"))
            except Exception as e:
                console.print(f"[{STYLE_ERROR}]Error uploading {jar_file}:[/]\n{traceback.format_exc()}")
                results.append(("jar", remote_path, "", f"FAILED: {e}"))
    return results


def _extract_jars_from_oci_archive(archive_path, release, artifact_store):
    """Extract JARs from an OCI image archive (docker save format) and upload to the artifact store."""
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        oci_dir = os.path.join(tmpdir, "oci")
        with tarfile.open(archive_path, "r") as tar:
            _safe_extractall(tar, oci_dir)

        manifest_path = os.path.join(oci_dir, "manifest.json")
        with open(manifest_path) as f:
            archive_manifests = json.load(f)

        if not archive_manifests:
            results.append(
                ("engine-jars", artifact_store, "", "FAILED: no manifests in engine archive")
            )
            return results

        entry = archive_manifests[0]

        jars_tmpdir = os.path.join(tmpdir, "jars_extract")
        os.makedirs(jars_tmpdir, exist_ok=True)

        for layer_file_rel in entry["Layers"]:
            layer_path = os.path.join(oci_dir, layer_file_rel)
            _extract_jars_from_layer(layer_path, jars_tmpdir)

        results.extend(
            _upload_jars_to_store(os.path.join(jars_tmpdir, "jars"), release, artifact_store)
        )

    return results


def _extract_engine_jars_from_bundle(bundle_path, cloud, release, artifact_store, progress):
    """Extract engine JARs from a bundle's engine image archive and copy them to the artifact store."""
    label = f"engine JARs (engine-{cloud}:{release})"
    task_id = progress.add_task(f"Extracting {label}", total=None)

    with tempfile.TemporaryDirectory() as tmpdir:
        with tarfile.open(bundle_path, "r:gz") as tar:
            _safe_extractall(tar, tmpdir)

        engine_archive = os.path.join(tmpdir, f"engine-{cloud}.tar")
        if not os.path.exists(engine_archive):
            _finish_task(progress, task_id, label, ok=False)
            return [
                (
                    "engine-jars",
                    artifact_store,
                    "",
                    f"FAILED: engine-{cloud}.tar not found in bundle",
                )
            ]

        results = _extract_jars_from_oci_archive(engine_archive, release, artifact_store)

    ok = all(status == "ok" for _, _, _, status in results)
    _finish_task(progress, task_id, label, ok=ok)
    return results


def _upload_engine_jars_to_store(client, registry, release, cloud, artifact_store, progress):
    """Extract JARs from the engine image and upload to a blob store."""
    results = []
    engine_repo = f"ziplineai/engine-{cloud}"

    # Engine images on Docker Hub are tagged with a "v" prefix (e.g. v1.0.19), unlike hub/frontend images.
    engine_release = release if release.startswith("v") or release == "latest" else f"v{release}"
    label = f"engine JARs ({engine_repo}:{engine_release})"

    try:
        manifest = client.resolve_single_platform(registry, engine_repo, engine_release)
    except RegistryError as e:
        console.print(f"[{STYLE_ERROR}]Error resolving {engine_repo}:{engine_release}:[/]\n{traceback.format_exc()}")
        results.append(("engine-jars", artifact_store, "", f"FAILED: {e}"))
        return results

    total_bytes = sum(layer.get("size", 0) for layer in manifest.get("layers", []))
    task_id = progress.add_task(f"Extracting {label}", total=total_bytes)

    with tempfile.TemporaryDirectory() as tmpdir:
        for layer in manifest.get("layers", []):
            layer_digest = layer["digest"]
            layer_path = os.path.join(tmpdir, "layer.tar")
            client.extract_blob_to_file(
                registry,
                engine_repo,
                layer_digest,
                layer_path,
                on_progress=partial(_advance_progress, progress, task_id),
            )
            _extract_jars_from_layer(layer_path, tmpdir)

        results.extend(_upload_jars_to_store(os.path.join(tmpdir, "jars"), release, artifact_store))

    ok = all(status == "ok" for _, _, _, status in results)
    _finish_task(progress, task_id, label, ok=ok)
    return results


# ── Auth helpers ──────────────────────────────────────────────────────


def _check_docker_available():
    """Verify that the Docker CLI is on PATH and the daemon is running."""
    if not shutil.which("docker"):
        raise click.UsageError(
            "Docker CLI not found on PATH. Install Docker to use --registry local."
        )
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
    except subprocess.CalledProcessError as exc:
        raise click.UsageError(
            "Docker daemon is not running. Start Docker to use --registry local."
        ) from exc


def _authenticate_docker_hub(client, api_token):
    """Authenticate the OCI client against Docker Hub using an API token or local Docker credentials."""
    if api_token:
        client.authenticate(DOCKER_HUB_REGISTRY, username="ziplineai", password=api_token)
        return

    username, password = _get_docker_credentials(DOCKER_HUB_REGISTRY)
    if username and password:
        client.authenticate(DOCKER_HUB_REGISTRY, username=username, password=password)
        return

    raise click.UsageError(
        "Docker Hub credentials not found. Provide --api-token or log in with 'docker login'."
    )


def _base_domain(server):
    """Extract base domain from a Docker config server key (e.g. 'https://index.docker.io/v1/' -> 'docker.io')."""
    from urllib.parse import urlparse

    parsed = urlparse(server if "://" in server else f"https://{server}")
    hostname = parsed.hostname or ""
    parts = hostname.rsplit(".", 2)
    return ".".join(parts[-2:]) if len(parts) >= 2 else hostname


def _get_docker_credentials(registry):
    """Read credentials for a registry from the local Docker credential store (~/.docker/config.json)."""
    config_path = os.path.join(os.path.expanduser("~"), ".docker", "config.json")
    if not os.path.exists(config_path):
        return None, None

    with open(config_path) as f:
        config = json.load(f)

    registry_domain = _base_domain(registry)

    for server, helper in config.get("credHelpers", {}).items():
        if _base_domain(server) == registry_domain:
            username, password = _creds_from_helper(helper, server)
            if username:
                return username, password

    creds_store = config.get("credsStore")
    if creds_store:
        for server in config.get("auths", {}):
            if _base_domain(server) == registry_domain:
                username, password = _creds_from_helper(creds_store, server)
                if username:
                    return username, password

    import base64

    for server, entry in config.get("auths", {}).items():
        if _base_domain(server) == registry_domain:
            auth_b64 = entry.get("auth")
            if auth_b64:
                try:
                    decoded = base64.b64decode(auth_b64).decode()
                except Exception:
                    continue
                if ":" not in decoded:
                    continue
                username, password = decoded.split(":", 1)
                return username, password

    return None, None


def _creds_from_helper(helper_name, server):
    """Get credentials from a Docker credential helper binary."""
    try:
        proc = subprocess.run(
            [f"docker-credential-{helper_name}", "get"],
            input=server,
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            creds = json.loads(proc.stdout)
            return creds.get("Username"), creds.get("Secret")
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return None, None


def _authenticate_target_registry(client, registry):
    """Set up auth for the target registry using ambient cloud credentials."""
    if "pkg.dev" in registry:
        try:
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True
            )
            client.authenticate(
                registry, username="oauth2accesstoken", password=result.stdout.strip()
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print(
                "[yellow]Warning: Could not get gcloud access token. Target registry auth may fail.[/yellow]"
            )
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
            console.print(
                "[yellow]Warning: Could not get ECR auth token. Target registry auth may fail.[/yellow]"
            )
    elif ".azurecr.io" in registry:
        try:
            result = subprocess.run(
                ["az", "acr", "login", "--name", registry.split(".")[0], "--expose-token"],
                capture_output=True,
                text=True,
                check=True,
            )
            token_data = json.loads(result.stdout)
            client.authenticate(
                registry,
                username="00000000-0000-0000-0000-000000000000",
                password=token_data["accessToken"],
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            console.print(
                "[yellow]Warning: Could not get ACR token. Target registry auth may fail.[/yellow]"
            )


# ── Output ────────────────────────────────────────────────────────────


def _print_summary(results, release, cloud, registry):
    """Print a summary table of the load operation."""
    is_local = registry == "local"
    title = (
        f"Zipline {release} ({cloud}) -> local Docker"
        if is_local
        else f"Zipline {release} ({cloud}) -> {registry}"
    )
    table = Table(title=title)
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
        if is_local:
            console.print("\nImages available in local Docker daemon:")
            console.print(f"  ziplineai/hub-{cloud}:{release}")
            if cloud in _CLOUDS_WITH_EVAL:
                console.print(f"  ziplineai/eval-{cloud}:{release}")
            console.print(f"  ziplineai/web-ui:{release}")
        else:
            console.print("\nFor terraform.tfvars:")
            console.print(f'  hub_image      = "{registry}/ziplineai/hub-{cloud}:{release}"')
            if cloud in _CLOUDS_WITH_EVAL:
                console.print(f'  eval_image     = "{registry}/ziplineai/eval-{cloud}:{release}"')
            console.print(f'  frontend_image = "{registry}/ziplineai/web-ui:{release}"')
            console.print(f'  engine_image   = "{registry}/ziplineai/engine-{cloud}:{release}"')
    else:
        console.print("\n[bold red]Some artifacts failed to load. See errors above.[/bold red]")
        raise SystemExit(1)


@admin.command("verify")
@click.argument("hub_url")
@click.option("--expected-version", default=None, help="Expected Zipline version (optional).")
def verify(hub_url, expected_version):
    """Check that a Zipline hub is reachable and healthy.

    HUB_URL is the URL of the running Zipline hub (e.g. https://hub.example.com).
    """
    import urllib3

    http = urllib3.PoolManager()
    hub_url = hub_url.rstrip("/")

    results = []

    console.print(f"[bold]Checking hub health at {hub_url}/debug...[/bold]")
    try:
        resp = http.request("GET", f"{hub_url}/debug", timeout=10.0)
        if resp.status == 200:
            body = json.loads(resp.data)
            actual_version = body.get("version", "unknown")
            results.append(("Hub Health", f"{hub_url}/debug", "ok", f"version={actual_version}"))

            if expected_version and actual_version != expected_version:
                results.append(
                    (
                        "Version Check",
                        "",
                        "WARN",
                        f"Expected {expected_version}, got {actual_version}",
                    )
                )
            elif expected_version:
                results.append(("Version Check", "", "ok", f"matches {expected_version}"))
        else:
            results.append(("Hub Health", f"{hub_url}/debug", "FAIL", f"HTTP {resp.status}"))
    except Exception as e:
        results.append(("Hub Health", f"{hub_url}/debug", "FAIL", str(e)))

    console.print(f"[bold]Checking upload API at {hub_url}/upload/v2/diff...[/bold]")
    try:
        resp = http.request("POST", f"{hub_url}/upload/v2/diff", timeout=10.0, body=b"{}")
        if resp.status < 500:
            results.append(
                ("Upload API", f"{hub_url}/upload/v2/diff", "ok", f"HTTP {resp.status} (reachable)")
            )
        else:
            results.append(
                ("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", f"HTTP {resp.status}")
            )
    except Exception as e:
        results.append(("Upload API", f"{hub_url}/upload/v2/diff", "FAIL", str(e)))

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
