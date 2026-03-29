"""Resolve a ZIPLINE_TOKEN environment variable to a usable JWT.

Supports two token types (auto-detected):
- **JWT** (from ``zipline auth get-access-token``): used directly, no exchange needed.
- **Session token** (from a service principal): exchanged for a short-lived JWT
  via ``GET {auth_url}/api/auth/token``.  Exchanged JWTs are cached and
  automatically refreshed when within 60 s of expiry.
"""

import base64
import json
import threading
import time

import requests


def is_jwt(token: str) -> bool:
    """Return True if *token* looks like a JWT (three base64url segments)."""
    parts = token.split(".")
    if len(parts) != 3:
        return False
    for part in parts:
        # Add base64url padding and attempt decode
        padded = part + "=" * (4 - len(part) % 4)
        try:
            base64.urlsafe_b64decode(padded)
        except Exception:
            return False
    return True


def _decode_jwt_exp(jwt_token: str) -> float:
    """Decode the ``exp`` claim from a JWT without signature verification."""
    payload_b64 = jwt_token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    payload = json.loads(base64.urlsafe_b64decode(payload_b64))
    return float(payload["exp"])


def exchange_session_for_jwt(session_token: str, auth_url: str) -> str:
    """Exchange a BetterAuth session token for a short-lived JWT.

    Calls ``GET {auth_url}/api/auth/token`` with the session token as a
    Bearer credential and returns the JWT string.
    """
    endpoint = f"{auth_url.rstrip('/')}/api/auth/token"
    try:
        resp = requests.get(
            endpoint,
            headers={"Authorization": f"Bearer {session_token}"},
            timeout=10,
        )
    except requests.ConnectionError:
        raise RuntimeError(
            f"Could not connect to auth server at {endpoint}. "
            "Check that ZIPLINE_AUTH_URL is correct and the frontend is running. "
            f"Current ZIPLINE_AUTH_URL: {auth_url}"
        ) from None
    except requests.Timeout:
        raise RuntimeError(
            f"Timed out connecting to auth server at {endpoint}. "
            "Check that ZIPLINE_AUTH_URL is correct and the frontend is reachable."
        ) from None

    if resp.status_code == 401:
        raise RuntimeError(
            f"Token rejected by auth server at {endpoint} (HTTP 401). "
            "The ZIPLINE_TOKEN may be expired, revoked, or does not belong to "
            "this auth server. Check that ZIPLINE_AUTH_URL matches the frontend "
            "where the token was created. If using a service principal, rotate "
            "the token in the admin UI. If using a personal session, run "
            "'zipline auth login' to re-authenticate."
        )
    if not resp.ok:
        raise RuntimeError(
            f"Token exchange failed (HTTP {resp.status_code}) at {endpoint}. "
            f"Response: {resp.text[:200]}"
        )

    token = resp.json().get("token")
    if not token:
        raise RuntimeError(
            f"Token exchange at {endpoint} succeeded but no JWT was returned. "
            "Check that the auth server is configured correctly."
        )
    return token


# ---------------------------------------------------------------------------
# Cached resolver
# ---------------------------------------------------------------------------

_cache: dict[tuple[str, str], dict] = {}  # (session_token, auth_url) -> {"jwt": str, "exp": float}
_cache_lock = threading.Lock()


def resolve_token(token: str, auth_url: str | None = None) -> str:
    """Resolve a ``ZIPLINE_TOKEN`` value to a JWT suitable for API requests.

    * If *token* is already a JWT, return it as-is.
    * If *token* is a session token, exchange it for a JWT via *auth_url*.
      Exchanged JWTs are cached and re-used until within 60 s of expiry.

    Raises ``ValueError`` if *token* is a session token and *auth_url* is not
    provided.
    """
    if is_jwt(token):
        return token

    if not auth_url:
        raise ValueError(
            "ZIPLINE_AUTH_URL must be set when ZIPLINE_TOKEN is a session token "
            "(not a JWT). Set ZIPLINE_AUTH_URL to the frontend URL "
            "(e.g. https://zipline.example.com)."
        )

    key = (token, auth_url)
    with _cache_lock:
        cached = _cache.get(key)
        if cached and cached["exp"] - time.time() > 60:
            return cached["jwt"]

    jwt = exchange_session_for_jwt(token, auth_url)
    exp = _decode_jwt_exp(jwt)
    with _cache_lock:
        _cache[key] = {"jwt": jwt, "exp": exp}
    return jwt
