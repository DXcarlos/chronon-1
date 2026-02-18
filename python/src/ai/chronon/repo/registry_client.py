"""OCI Distribution API HTTP client for copying images between registries.

Implements the OCI Distribution Spec (v2) for pulling/pushing manifests and blobs.
Uses urllib3 (already a dependency) — no Docker CLI or daemon required.
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field

import urllib3

logger = logging.getLogger(__name__)

# Docker Hub uses a separate auth service
DOCKER_HUB_REGISTRY = "registry-1.docker.io"
DOCKER_HUB_AUTH_URL = "https://auth.docker.io/token"

# OCI media types
MANIFEST_V2 = "application/vnd.docker.distribution.manifest.v2+json"
MANIFEST_LIST_V2 = "application/vnd.docker.distribution.manifest.list.v2+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
ALL_MANIFEST_TYPES = ", ".join([MANIFEST_V2, MANIFEST_LIST_V2, OCI_MANIFEST, OCI_INDEX])

# Stream blobs in 8MB chunks to avoid loading multi-GB layers into memory
BLOB_CHUNK_SIZE = 8 * 1024 * 1024


@dataclass
class RegistryAuth:
    """Holds auth state for a single registry."""

    token: str | None = None
    username: str | None = None
    password: str | None = None
    scheme: str = "Bearer"


@dataclass
class RegistryClient:
    """Thin OCI registry HTTP client.

    Handles Docker Hub token exchange and generic Bearer token auth.
    Streams blobs to avoid loading multi-GB layers into memory.
    """

    http: urllib3.PoolManager = field(default_factory=lambda: urllib3.PoolManager(num_pools=4, maxsize=10))
    _auth_cache: dict[str, RegistryAuth] = field(default_factory=dict)

    def authenticate(self, registry: str, username: str | None = None, password: str | None = None) -> None:
        """Store credentials for a registry. Token exchange happens lazily on first request."""
        self._auth_cache[registry] = RegistryAuth(username=username, password=password)

    def _get_auth_header(self, registry: str, repo: str) -> dict[str, str]:
        """Get Authorization header, performing token exchange if needed."""
        auth = self._auth_cache.get(registry)
        if not auth:
            return {}

        if auth.token:
            return {"Authorization": f"{auth.scheme} {auth.token}"}

        # Docker Hub uses a separate token service
        if registry == DOCKER_HUB_REGISTRY:
            return self._docker_hub_token_exchange(auth, repo)

        # For other registries, use Basic auth if we have credentials
        if auth.username and auth.password:
            headers = urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}")
            return dict(headers)

        return {}

    def _docker_hub_token_exchange(self, auth: RegistryAuth, repo: str) -> dict[str, str]:
        """Exchange Docker Hub PAT for a short-lived Bearer token."""
        headers = {}
        if auth.username and auth.password:
            headers = dict(urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}"))

        resp = self.http.request(
            "GET",
            DOCKER_HUB_AUTH_URL,
            fields={"service": "registry.docker.io", "scope": f"repository:{repo}:pull"},
            headers=headers,
        )
        if resp.status != 200:
            raise RegistryError(f"Docker Hub auth failed (HTTP {resp.status}): {resp.data.decode()}")

        token = json.loads(resp.data)["token"]
        auth.token = token
        return {"Authorization": f"Bearer {token}"}

    def _registry_token_exchange(self, registry: str, repo: str, www_authenticate: str) -> dict[str, str]:
        """Handle WWW-Authenticate challenge for non-Docker-Hub registries."""
        auth = self._auth_cache.get(registry, RegistryAuth())

        # Parse WWW-Authenticate: Bearer realm="...",service="...",scope="..."
        params = {}
        if "Bearer " in www_authenticate:
            parts = www_authenticate.split("Bearer ", 1)[1]
            for part in parts.split(","):
                key, _, value = part.strip().partition("=")
                params[key] = value.strip('"')

        realm = params.get("realm", "")
        if not realm:
            if auth.username and auth.password:
                headers = urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}")
                return dict(headers)
            return {}

        fields = {}
        if "service" in params:
            fields["service"] = params["service"]
        if "scope" in params:
            fields["scope"] = params["scope"]
        else:
            fields["scope"] = f"repository:{repo}:pull,push"

        headers = {}
        if auth.username and auth.password:
            headers = dict(urllib3.make_headers(basic_auth=f"{auth.username}:{auth.password}"))

        resp = self.http.request("GET", realm, fields=fields, headers=headers)
        if resp.status != 200:
            raise RegistryError(f"Token exchange failed at {realm} (HTTP {resp.status}): {resp.data.decode()}")

        data = json.loads(resp.data)
        token = data.get("token") or data.get("access_token")
        if token:
            auth.token = token
            self._auth_cache[registry] = auth
            return {"Authorization": f"Bearer {token}"}

        return {}

    def _request(
        self,
        method: str,
        registry: str,
        path: str,
        repo: str,
        headers: dict | None = None,
        body: bytes | None = None,
        preload_content: bool = True,
    ) -> urllib3.HTTPResponse:
        """Make an authenticated request to a registry, handling 401 challenges."""
        url = f"https://{registry}{path}"
        hdrs = self._get_auth_header(registry, repo)
        if headers:
            hdrs.update(headers)

        resp = self.http.request(method, url, headers=hdrs, body=body, preload_content=preload_content)

        # Handle 401 with WWW-Authenticate challenge
        if resp.status == 401:
            www_auth = resp.headers.get("WWW-Authenticate", "")
            if www_auth:
                hdrs = self._registry_token_exchange(registry, repo, www_auth)
                if headers:
                    hdrs.update(headers)
                resp = self.http.request(method, url, headers=hdrs, body=body, preload_content=preload_content)

        return resp

    def get_manifest(self, registry: str, repo: str, reference: str) -> tuple[bytes, str, str]:
        """Pull a manifest by tag or digest. Returns (body, content_type, digest)."""
        resp = self._request(
            "GET",
            registry,
            f"/v2/{repo}/manifests/{reference}",
            repo,
            headers={"Accept": ALL_MANIFEST_TYPES},
        )
        if resp.status != 200:
            raise RegistryError(f"Failed to get manifest {repo}:{reference} from {registry} (HTTP {resp.status})")

        content_type = resp.headers.get("Content-Type", MANIFEST_V2)
        digest = resp.headers.get("Docker-Content-Digest", "")
        if not digest:
            digest = "sha256:" + hashlib.sha256(resp.data).hexdigest()

        return resp.data, content_type, digest

    def put_manifest(self, registry: str, repo: str, reference: str, body: bytes, content_type: str) -> str:
        """Push a manifest. Returns the digest."""
        resp = self._request(
            "PUT",
            registry,
            f"/v2/{repo}/manifests/{reference}",
            repo,
            headers={"Content-Type": content_type},
            body=body,
        )
        if resp.status not in (201, 202):
            raise RegistryError(
                f"Failed to push manifest {repo}:{reference} to {registry} (HTTP {resp.status}): {resp.data.decode()}"
            )

        digest = resp.headers.get("Docker-Content-Digest", "")
        if not digest:
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
        return digest

    def blob_exists(self, registry: str, repo: str, digest: str) -> bool:
        """Check if a blob already exists in the target registry (HEAD request)."""
        resp = self._request("HEAD", registry, f"/v2/{repo}/blobs/{digest}", repo)
        return resp.status == 200

    def pull_blob(self, registry: str, repo: str, digest: str) -> bytes:
        """Pull a blob by digest. For small blobs (configs). Use pull_blob_stream for layers."""
        resp = self._request("GET", registry, f"/v2/{repo}/blobs/{digest}", repo)
        if resp.status != 200:
            raise RegistryError(f"Failed to pull blob {digest} from {registry}/{repo} (HTTP {resp.status})")
        return resp.data

    def pull_blob_stream(self, registry: str, repo: str, digest: str) -> urllib3.HTTPResponse:
        """Pull a blob as a streaming response."""
        resp = self._request(
            "GET", registry, f"/v2/{repo}/blobs/{digest}", repo, preload_content=False
        )
        if resp.status != 200:
            raise RegistryError(f"Failed to pull blob {digest} from {registry}/{repo} (HTTP {resp.status})")
        return resp

    def push_blob(self, registry: str, repo: str, digest: str, data: bytes) -> None:
        """Push a small blob (config) via monolithic upload."""
        resp = self._request("POST", registry, f"/v2/{repo}/blobs/uploads/", repo)
        if resp.status not in (200, 202):
            raise RegistryError(f"Failed to initiate blob upload to {registry}/{repo} (HTTP {resp.status})")

        location = resp.headers.get("Location", "")
        if not location:
            raise RegistryError("No Location header in upload initiation response")

        separator = "&" if "?" in location else "?"
        upload_url = f"{location}{separator}digest={digest}"
        if upload_url.startswith("/"):
            upload_url = f"https://{registry}{upload_url}"

        hdrs = self._get_auth_header(registry, repo)
        hdrs["Content-Type"] = "application/octet-stream"
        hdrs["Content-Length"] = str(len(data))

        resp = self.http.request("PUT", upload_url, headers=hdrs, body=data)
        if resp.status not in (201, 202):
            raise RegistryError(
                f"Failed to push blob {digest} to {registry}/{repo} (HTTP {resp.status}): {resp.data.decode()}"
            )

    def push_blob_stream(self, registry: str, repo: str, digest: str, source_stream: urllib3.HTTPResponse,
                         content_length: int) -> None:
        """Push a large blob by streaming from a source response."""
        resp = self._request("POST", registry, f"/v2/{repo}/blobs/uploads/", repo)
        if resp.status not in (200, 202):
            raise RegistryError(f"Failed to initiate blob upload to {registry}/{repo} (HTTP {resp.status})")

        location = resp.headers.get("Location", "")
        if not location:
            raise RegistryError("No Location header in upload initiation response")

        separator = "&" if "?" in location else "?"
        upload_url = f"{location}{separator}digest={digest}"
        if upload_url.startswith("/"):
            upload_url = f"https://{registry}{upload_url}"

        data = source_stream.read()
        source_stream.release_conn()

        hdrs = self._get_auth_header(registry, repo)
        hdrs["Content-Type"] = "application/octet-stream"
        hdrs["Content-Length"] = str(len(data))

        resp = self.http.request("PUT", upload_url, headers=hdrs, body=data)
        if resp.status not in (201, 202):
            raise RegistryError(
                f"Failed to push blob {digest} to {registry}/{repo} (HTTP {resp.status}): {resp.data.decode()}"
            )

    def copy_image(self, src_registry: str, src_repo: str, dst_registry: str, dst_repo: str, tag: str) -> str:
        """Copy an image (manifest + all blobs) from source to destination registry.

        Handles both single-platform manifests and multi-platform manifest lists/OCI indexes.
        Returns the manifest digest.
        """
        logger.info(f"Copying {src_registry}/{src_repo}:{tag} -> {dst_registry}/{dst_repo}:{tag}")

        manifest_bytes, content_type, digest = self.get_manifest(src_registry, src_repo, tag)
        manifest = json.loads(manifest_bytes)

        if content_type in (MANIFEST_LIST_V2, OCI_INDEX):
            for platform_manifest in manifest.get("manifests", []):
                platform_digest = platform_manifest["digest"]
                self._copy_single_manifest(src_registry, src_repo, dst_registry, dst_repo, platform_digest)
        else:
            self._copy_blobs_for_manifest(src_registry, src_repo, dst_registry, dst_repo, manifest)

        result_digest = self.put_manifest(dst_registry, dst_repo, tag, manifest_bytes, content_type)
        logger.info(f"Pushed manifest {dst_registry}/{dst_repo}:{tag} ({result_digest})")
        return result_digest

    def _copy_single_manifest(
        self, src_registry: str, src_repo: str, dst_registry: str, dst_repo: str, digest: str
    ) -> None:
        """Copy a single-platform manifest and its blobs."""
        manifest_bytes, content_type, _ = self.get_manifest(src_registry, src_repo, digest)
        manifest = json.loads(manifest_bytes)

        self._copy_blobs_for_manifest(src_registry, src_repo, dst_registry, dst_repo, manifest)
        self.put_manifest(dst_registry, dst_repo, digest, manifest_bytes, content_type)

    def _copy_blobs_for_manifest(
        self, src_registry: str, src_repo: str, dst_registry: str, dst_repo: str, manifest: dict
    ) -> None:
        """Copy all blobs (config + layers) referenced by a manifest."""
        config_digest = manifest.get("config", {}).get("digest")
        if config_digest:
            self._copy_blob(src_registry, src_repo, dst_registry, dst_repo, config_digest)

        for layer in manifest.get("layers", []):
            self._copy_blob(src_registry, src_repo, dst_registry, dst_repo, layer["digest"])

    def _copy_blob(
        self, src_registry: str, src_repo: str, dst_registry: str, dst_repo: str, digest: str
    ) -> None:
        """Copy a single blob, skipping if it already exists at the destination."""
        if self.blob_exists(dst_registry, dst_repo, digest):
            logger.debug(f"Blob {digest[:19]}... already exists at {dst_registry}/{dst_repo}, skipping")
            return

        logger.info(f"Copying blob {digest[:19]}...")
        stream = self.pull_blob_stream(src_registry, src_repo, digest)
        content_length = int(stream.headers.get("Content-Length", 0))
        self.push_blob_stream(dst_registry, dst_repo, digest, stream, content_length)

    def extract_blob_to_file(self, registry: str, repo: str, digest: str, output_path: str) -> None:
        """Download a blob to a local file. Used for extracting JARs from engine image layers."""
        resp = self._request("GET", registry, f"/v2/{repo}/blobs/{digest}", repo, preload_content=False)
        if resp.status != 200:
            raise RegistryError(f"Failed to pull blob {digest} (HTTP {resp.status})")

        with open(output_path, "wb") as f:
            while True:
                chunk = resp.read(BLOB_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
        resp.release_conn()


class RegistryError(Exception):
    """Raised when an OCI registry operation fails."""
