"""zipline admin generate — YAML-driven K8s config generation for local kind."""
import os
import shutil
from pathlib import Path

import click
from importlib_resources import files
from jinja2 import Environment, FileSystemLoader

from ai.chronon.cli.theme import console, print_success
from ai.chronon.local_config import StackConfig, load_config

_ONLINE_CLASS_MAP = {
    "redis": "ai.chronon.integrations.cloud_k8s.LocalRedisApiImpl",
    "local_kv": "ai.chronon.integrations.cloud_k8s.LocalTestApiImpl",
}

_OTEL_FILE_MAP = {
    "none": None,
    "local": "otel-local.yaml",
    "gcp": "otel-gcp.yaml",
    "aws": "otel-aws.yaml",
    "azure": "otel-azure.yaml",
}

_RESOURCES_DIR = Path(str(files("ai.chronon").joinpath("resources", "local")))


def _render_template(template_path: Path, context: dict) -> str:
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        keep_trailing_newline=True,
    )
    return env.get_template(template_path.name).render(**context)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def _copy_static(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)


def _image_prefix(config: StackConfig) -> str:
    registry = config.images.registry
    if registry == "local":
        return ""
    return registry.rstrip("/") + "/"


def _render_kind_config(config: StackConfig, output_dir: Path) -> None:
    tmpl = _RESOURCES_DIR / "kind" / "cluster-config.yaml.j2"
    rendered = _render_template(tmpl, {"cluster_name": config.cluster.name})
    _write(output_dir / "kind" / "cluster-config.yaml", rendered)


def _copy_static_base_files(output_dir: Path) -> None:
    static_files = [
        "namespace.yaml",
        "spark-rbac.yaml",
        "postgres.yaml",
        "minio.yaml",
        "starrocks.yaml",
        "loki.yaml",
        "promtail.yaml",
        "spark-history-server.yaml",
        "redis.yaml",
        "eval.yaml",
        "frontend.yaml",
    ]
    src_base = _RESOURCES_DIR / "kustomize" / "base"
    dest_base = output_dir / "kustomize" / "base"
    dest_base.mkdir(parents=True, exist_ok=True)
    for fname in static_files:
        src = src_base / fname
        if src.exists():
            _copy_static(src, dest_base / fname)


def _render_base_kustomization(config: StackConfig, output_dir: Path, otel_file) -> None:
    tmpl = _RESOURCES_DIR / "kustomize" / "base" / "kustomization.yaml.j2"
    rendered = _render_template(tmpl, {
        "online_store": config.online_store,
        "otel_file": otel_file,
    })
    _write(output_dir / "kustomize" / "base" / "kustomization.yaml", rendered)


def _render_hub(config: StackConfig, output_dir: Path) -> None:
    tmpl = _RESOURCES_DIR / "kustomize" / "base" / "hub.yaml.j2"
    prefix = _image_prefix(config)
    rendered = _render_template(tmpl, {
        "online_class": _ONLINE_CLASS_MAP[config.online_store.type],
        "spark_image": f"{prefix}chronon-spark-k8s:{config.images.tag}",
        "redis_host": config.online_store.host if config.online_store.type == "redis" else "",
        "pull_policy": config.images.pull_policy,
    })
    _write(output_dir / "kustomize" / "base" / "hub.yaml", rendered)


def _render_fetcher(config: StackConfig, output_dir: Path, metrics_enabled: bool) -> None:
    tmpl = _RESOURCES_DIR / "kustomize" / "base" / "fetcher.yaml.j2"
    rendered = _render_template(tmpl, {
        "online_class": _ONLINE_CLASS_MAP[config.online_store.type],
        "redis_host": config.online_store.host if config.online_store.type == "redis" else "",
        "pull_policy": config.images.pull_policy,
        "metrics_enabled": metrics_enabled,
    })
    _write(output_dir / "kustomize" / "base" / "fetcher.yaml", rendered)


def _render_otel(config: StackConfig, output_dir: Path, otel_file) -> None:
    if otel_file is None:
        return

    dest = output_dir / "kustomize" / "base" / otel_file
    obs = config.observability

    if obs.type == "local":
        _copy_static(_RESOURCES_DIR / "kustomize" / "base" / "otel-local.yaml", dest)
    else:
        tmpl = _RESOURCES_DIR / "kustomize" / "base" / f"{otel_file}.j2"
        rendered = _render_template(tmpl, {"observability": obs})
        _write(dest, rendered)


def _render_overlays(config: StackConfig, output_dir: Path) -> None:
    prefix = _image_prefix(config)
    ctx = {"config": config, "image_prefix": prefix}
    for overlay in ("local-dev", "quickstart"):
        tmpl = _RESOURCES_DIR / "kustomize" / "overlays" / overlay / "kustomization.yaml.j2"
        rendered = _render_template(tmpl, ctx)
        _write(output_dir / "kustomize" / "overlays" / overlay / "kustomization.yaml", rendered)


def _render_configs(config: StackConfig, output_dir: Path) -> None:
    otel_file = _OTEL_FILE_MAP[config.observability.type]
    metrics_enabled = otel_file is not None

    _render_kind_config(config, output_dir)
    _copy_static_base_files(output_dir)
    _render_base_kustomization(config, output_dir, otel_file)
    _render_hub(config, output_dir)
    _render_fetcher(config, output_dir, metrics_enabled)
    _render_otel(config, output_dir, otel_file)
    _render_overlays(config, output_dir)


def _check_tools() -> bool:
    ok = True
    for tool in ("kind", "docker", "kubectl"):
        if shutil.which(tool):
            console.print(f"  [green]✓[/green] {tool}")
        else:
            console.print(f"  [red]✗[/red] {tool} not found on PATH")
            ok = False
    return ok


@click.command("generate")
@click.option(
    "--config", "config_path",
    default="./zipline-stack.yaml", show_default=True,
    type=click.Path(),
    help="Path to zipline-stack.yaml",
)
@click.option(
    "--output-dir",
    default="./k8s-local", show_default=True,
    type=click.Path(),
    help="Directory to write generated configs",
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing output directory")
@click.option("--check-tools", is_flag=True, help="Only check kind/docker/kubectl installation, then exit")
def generate(config_path, output_dir, overwrite, check_tools):
    """Generate Kubernetes + kind configs from zipline-stack.yaml."""
    if check_tools:
        console.print("[bold]Checking required tools...[/bold]")
        ok = _check_tools()
        if not ok:
            raise SystemExit(1)
        return

    if not os.path.exists(config_path):
        raise click.UsageError(
            f"Config file not found: {config_path}\n"
            "Run 'zipline admin init k8s' to create one."
        )

    try:
        config = load_config(config_path)
    except (ValueError, KeyError) as e:
        raise click.UsageError(f"Invalid config: {e}") from e

    output_path = Path(os.path.abspath(output_dir))
    if output_path.exists() and not overwrite:
        raise click.UsageError(
            f"Output directory already exists: {output_path}\n"
            "Use --overwrite to replace it."
        )

    if output_path.exists() and overwrite:
        shutil.rmtree(output_path)

    console.print(f"[bold]Generating configs into {output_path}...[/bold]")
    _render_configs(config, output_path)
    print_success("Configs generated.")

    registry = config.images.registry
    if registry == "local":
        overlay = "local-dev"
    else:
        overlay = "quickstart"

    console.print("\nNext steps:")
    console.print(f"\n  kubectl apply -k {output_path}/kustomize/overlays/{overlay}")
    console.print(
        "\n  If you're using a local kind cluster, create it and load images first:"
    )
    console.print(
        f"    kind create cluster --name {config.cluster.name} "
        f"--config {output_path}/kind/cluster-config.yaml"
    )
    if registry == "local":
        tag = config.images.tag
        images = " ".join(
            f"{img}:{tag}"
            for img in ("chronon-spark-k8s", "chronon-spark-base", "chronon-frontend",
                        "chronon-fetcher-k8s")
        )
        console.print(
            f"    kind load docker-image {images} --name {config.cluster.name}"
        )
