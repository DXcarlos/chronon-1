import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests

from ai.chronon.cli.formatter import Format, format_print


class ZiplineHub:
    def __init__(self, base_url, sa_name=None, use_auth=False, eval_url=None, cloud_provider=None, scope=None, format: Format = Format.TEXT):
        if not base_url:
            raise ValueError("Base URL for ZiplineHub cannot be empty.")
        self.base_url = base_url
        self.eval_url = eval_url
        self.format = format
        self.cloud_provider = cloud_provider.lower() if cloud_provider is not None else cloud_provider
        if self.base_url.startswith("https") and use_auth:
            if self.cloud_provider == "gcp":
                self.use_auth = True
                self._setup_gcp_auth(sa_name)
            elif self.cloud_provider == "azure":
                if not scope:
                    raise ValueError("Azure auth requires a non-empty scope.")
                self.use_auth = True
                self.sa = None
                self._setup_azure_auth(scope)
            else:
                # For non-GCP clouds, check for generic token
                self.id_token = os.getenv("ID_TOKEN")
                if not self.id_token:
                    # Disable auth if ID_TOKEN is not available for non-GCP clouds
                    self.use_auth = False
                    self.id_token = None
                    self.sa = None
                    format_print("\n ⚠️  No ID_TOKEN found in environment for non-GCP cloud provider. Disabling authentication for ZiplineHub.", format=format)
                else:
                    self.use_auth = True
                    format_print("\n 🔐 Using authentication for ZiplineHub.", format=format)
                    self.sa = None
        else:
            self.use_auth = False
            format_print("\n 🔓 Not using authentication for ZiplineHub.", format=format)

    def _setup_gcp_auth(self, sa_name):
        """Setup Google Cloud authentication."""
        import google.auth
        from google.auth.transport.requests import Request

        format_print("\n 🔐 Using Google Cloud authentication for ZiplineHub.", format=self.format)

        # First try to get ID token from environment (GitHub Actions)
        self.id_token = os.getenv("GCP_ID_TOKEN")
        if self.id_token:
            format_print(" 🔑 Using ID token from environment", format=self.format)
            self.sa = None
        elif sa_name is not None:
            # Fallback to Google Cloud authentication
            format_print(" 🔑 Generating ID token from service account credentials", format=self.format)
            credentials, project_id = google.auth.default()
            self.project_id = project_id
            credentials.refresh(Request())

            self.sa = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
        else:
            format_print(" 🔑 Generating ID token from default credentials", format=self.format)
            credentials, project_id = google.auth.default()
            credentials.refresh(Request())
            self.sa = None
            self.id_token = credentials.id_token

    def _setup_azure_auth(self, scope):
        """Setup Azure authentication."""
        from azure.core.exceptions import ClientAuthenticationError
        from azure.identity import AzureCliCredential, CredentialUnavailableError

        format_print("\n 🔐 Using Azure authentication for ZiplineHub.", format=self.format)
        format_print(f"Acquiring token from cli credentials for scope: {scope}...", format=self.format)
        try:
            credential = AzureCliCredential()

            # Request the token
            token_object = credential.get_token(scope)
            self.id_token = token_object.token

            format_print("   Success! Token acquired.", format=self.format)
        except (ClientAuthenticationError, CredentialUnavailableError) as e:
            format_print(f"   Error: Could not acquire token. Make sure you are logged in via 'az login'.\n   Details: {e}", format=self.format)
            self.use_auth = False
            self.id_token = None
            return

    def auth_headers(self, url):
        headers = {"Content-Type": "application/json"}
        if self.use_auth and hasattr(self, "sa") and self.sa is not None:
            headers["Authorization"] = f"Bearer {self._sign_jwt(self.sa, url)}"
        elif self.use_auth:
            headers["Authorization"] = f"Bearer {self.id_token}"
        return headers

    def handle_unauth(self, e: requests.RequestException, api_name: str):
        if e.response is not None and e.response.status_code == 401 and self.sa is None:
            format_print(
                f" ❌  Error calling {api_name} API. Unauthorized and no service account provided. Make sure the environment has default credentials set up or provide a service account name as SA_NAME in teams.py."
            , format=self.format)
        elif e.response is not None and e.response.status_code == 401 and self.sa is not None:
            format_print(
                f" ❌  Error calling {api_name} API. Unauthorized with provided service account: {self.sa}. Make sure the service account has the 'iap.webServiceVersions.accessViaIap' permission."
            , format=self.format)

    def _generate_jwt_payload(self, service_account_email: str, resource_url: str) -> str:
        """Generates JWT payload for service account.

        Creates a properly formatted JWT payload with standard claims (iss, sub, aud,
        iat, exp) needed for IAP authentication.

        Args:
            service_account_email (str): Specifies service account JWT is created for.
            resource_url (str): Specifies scope of the JWT, the URL that the JWT will
                be allowed to access.

        Returns:
            str: JSON string containing the JWT payload with properly formatted claims.
        """
        # Create current time and expiration time (1 hour later) in UTC
        iat = datetime.now(tz=timezone.utc)
        exp = iat + timedelta(seconds=3600)

        # Convert datetime objects to numeric timestamps (seconds since epoch)
        # as required by JWT standard (RFC 7519)
        payload = {
            "iss": service_account_email,
            "sub": service_account_email,
            "aud": resource_url,
            "iat": int(iat.timestamp()),
            "exp": int(exp.timestamp()),
        }

        return json.dumps(payload)

    def _sign_jwt(self, target_sa: str, resource_url: str) -> str:
        """Signs JWT payload using ADC and IAM credentials API.

        Uses Google Cloud's IAM Credentials API to sign a JWT. This requires the
        caller to have iap.webServiceVersions.accessViaIap permission on the target
        service account.

        Args:
            target_sa (str): Service Account JWT is being created for.
                iap.webServiceVersions.accessViaIap permission is required.
            resource_url (str): Audience of the JWT, and scope of the JWT token.
                This is the url of the IAP protected application.

        Returns:
            str: A signed JWT that can be used to access IAP protected apps.
                Use in Authorization header as: 'Bearer <signed_jwt>'
        """
        import google.auth
        from google.cloud import iam_credentials_v1

        # Get default credentials from environment or application credentials
        source_credentials, project_id = google.auth.default()

        # Initialize IAM credentials client with source credentials
        iam_client = iam_credentials_v1.IAMCredentialsClient(credentials=source_credentials)

        # Generate the service account resource name
        # Use '-' as placeholder as per API requirements
        name = iam_client.service_account_path("-", target_sa)

        # Create and sign the JWT payload
        payload = self._generate_jwt_payload(target_sa, resource_url)

        request = iam_credentials_v1.SignJwtRequest(
            name=name,
            payload=payload,
        )
        # Sign the JWT using the IAM credentials API
        response = iam_client.sign_jwt(request=request)

        return response.signed_jwt

    def call_diff_api(self, names_to_hashes: dict[str, str]) -> Optional[list[str]]:
        url = f"{self.base_url}/upload/v2/diff"

        diff_request = {"namesToHashes": names_to_hashes}
        try:
            response = requests.post(url, json=diff_request, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "diff")
            format_print(f" ❌ Error calling diff API: {e}", format=self.format)
            raise e

    def call_upload_api(self, diff_confs, branch: str):
        url = f"{self.base_url}/upload/v2/confs"

        upload_request = {
            "diffConfs": diff_confs,
            "branch": branch,
        }

        try:
            response = requests.post(url, json=upload_request, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "upload")
            format_print(f" ❌ Error calling upload API: {e}", format=self.format)
            raise e

    def call_schedule_api(self, modes, branch, conf_name, conf_hash):
        url = f"{self.base_url}/schedule/v2/schedules"

        schedule_request = {
            "modeSchedules": modes,
            "branch": branch,
            "confName": conf_name,
            "confHash": conf_hash,
        }

        try:
            response = requests.post(url, json=schedule_request, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "schedule deploy")
            format_print(f" ❌ Error deploying schedule: {e}", format=self.format)
            raise e

    def call_cancel_api(self, workflow_id):
        url = f"{self.base_url}/workflow/v2/{workflow_id}/cancel"

        try:
            response = requests.post(url, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "workflow cancel")
            format_print(f" ❌ Error calling workflow cancel API: {e}", format=self.format)
            raise e

    def call_sync_api(self, branch: str, names_to_hashes: dict[str, str]) -> Optional[list[str]]:
        url = f"{self.base_url}/upload/v2/sync"

        sync_request = {
            "namesToHashes": names_to_hashes,
            "branch": branch,
        }

        try:
            response = requests.post(url, json=sync_request, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "sync")
            format_print(f" ❌ Error calling sync API: {e}", format=self.format)
            raise e

    def call_eval_api(
        self,
        conf_name,
        conf_hash_map,
        parameters=None,
        ):
        if not self.eval_url:
            raise ValueError(" ❌ Eval URL not specified. Please specify EVAL_URL in teams.py, environment variables, or use the --eval-url flag.")
        _request = {
            "confName": conf_name,
            "confHashMap": conf_hash_map,
        }
        if parameters:
            _request["parameters"] = parameters
        try:
            response = requests.post(self.eval_url + "/eval", json=_request, headers=self.auth_headers(self.eval_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "eval")
            format_print(f" ❌ Error calling eval API: {e}", format=self.format)
            raise e

    def call_workflow_start_api(
        self,
        conf_name,
        mode,
        branch,
        user,
        conf_hash,
        start=None,
        end=None,
        skip_long_running=False,
    ):
        url = f"{self.base_url}/workflow/v2/start"
        end_dt = end.strftime("%Y-%m-%d") if end else date.today().strftime("%Y-%m-%d")
        start_dt = (
            start.strftime("%Y-%m-%d")
            if start
            else (date.today() - timedelta(days=14)).strftime("%Y-%m-%d")
        )
        workflow_request = {
            "confName": conf_name,
            "confHash": conf_hash,
            "mode": mode,
            "branch": branch,
            "user": user,
            "start": start_dt,
            "end": end_dt,
            "skipLongRunningNodes": skip_long_running,
        }
        try:
            response = requests.post(url, json=workflow_request, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "workflow start")
            format_print(f" ❌ Error calling workflow start API: {e}", format=self.format)
            raise e
