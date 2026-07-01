import base64
import json
from unittest.mock import Mock, patch

import pytest
import requests

from ai.chronon.repo.token_exchange import decode_jwt_claims, exchange_session_for_jwt


def _resp(status_code, *, json_body=None):
    resp = Mock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.json.return_value = json_body or {}
    resp.text = json.dumps(json_body or {})
    return resp


def test_decode_jwt_claims():
    payload = {"email": "a@b.com", "name": "A B", "role": "admin", "exp": 123}
    b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    assert decode_jwt_claims(f"header.{b64}.sig") == payload


def test_exchange_posts_token_in_body_without_authorization_header():
    """The opaque token must ride in the body, never the Authorization header —
    that is the whole point (JWT-only gateways reject non-JWT bearer tokens)."""
    post_resp = _resp(200, json_body={"token": "jwt-123"})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp) as mock_post,
        patch("ai.chronon.repo.token_exchange.requests.get") as mock_get,
    ):
        token = exchange_session_for_jwt("sess-abc", "https://hub.example.com")

    assert token == "jwt-123"
    mock_get.assert_not_called()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://hub.example.com/auth/cli-token"
    assert kwargs["json"] == {"session_token": "sess-abc"}
    assert "Authorization" not in (kwargs.get("headers") or {})


def test_exchange_strips_trailing_slash():
    post_resp = _resp(200, json_body={"token": "jwt-123"})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp) as mock_post,
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        exchange_session_for_jwt("sess-abc", "https://hub.example.com/")
    assert mock_post.call_args[0][0] == "https://hub.example.com/auth/cli-token"


def test_exchange_falls_back_to_legacy_endpoint_on_404():
    """Older frontends predate /auth/cli-token — fall back to the legacy
    bearer exchange so a CLI upgrade doesn't break against an old server."""
    post_resp = _resp(404)
    get_resp = _resp(200, json_body={"token": "legacy-jwt"})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp),
        patch("ai.chronon.repo.token_exchange.requests.get", return_value=get_resp) as mock_get,
    ):
        token = exchange_session_for_jwt("sess-abc", "https://hub.example.com")

    assert token == "legacy-jwt"
    args, kwargs = mock_get.call_args
    assert args[0] == "https://hub.example.com/api/auth/token"
    assert kwargs["headers"]["Authorization"] == "Bearer sess-abc"


def test_exchange_raises_on_401():
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=_resp(401)),
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        with pytest.raises(RuntimeError, match="expired"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")


def test_exchange_raises_on_connection_error():
    with patch(
        "ai.chronon.repo.token_exchange.requests.post",
        side_effect=requests.ConnectionError("boom"),
    ):
        with pytest.raises(RuntimeError, match="Could not connect"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")


def test_exchange_raises_when_no_token_returned():
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=_resp(200, json_body={})),
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        with pytest.raises(RuntimeError, match="no JWT was returned"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")
