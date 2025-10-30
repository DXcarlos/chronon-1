import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import google.auth
import requests
from google.auth.transport.requests import Request
from google.cloud import iam_credentials_v1


class ZiplineHub:
    def __init__(self, base_url, sa_name=None, use_auth=False, eval_url=None):
        if not base_url:
            raise ValueError("Base URL for ZiplineHub cannot be empty.")
        self.base_url = base_url
        self.eval_url = eval_url
        if self.base_url.startswith("https") or use_auth:
            self.use_auth = True
            print("\n 🔐 Using Google Cloud authentication for ZiplineHub.")

            # First try to get ID token from environment (GitHub Actions)
            self.id_token = os.getenv("GCP_ID_TOKEN")
            if self.id_token:
                print(" 🔑 Using ID token from environment")
            elif sa_name is not None:
                # Fallback to Google Cloud authentication
                print(" 🔑 Generating ID token from service account credentials")
                credentials, project_id = google.auth.default()
                self.project_id = project_id
                credentials.refresh(Request())

                self.sa = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
            else:
                print(" 🔑 Generating ID token from default credentials")
                credentials, project_id = google.auth.default()
                credentials.refresh(Request())
                self.sa = None
                self.id_token = credentials.id_token
        else:
            self.use_auth = False
            print("\n 🔓 Not using authentication for ZiplineHub.")

    def auth_headers(self, url):
        headers = {"Content-Type": "application/json"}
        if self.use_auth and hasattr(self, "sa") and self.sa is not None:
            headers["Authorization"] = f"Bearer {self._sign_jwt(self.sa, url)}"
        elif self.use_auth:
            headers["Authorization"] = f"Bearer {self.id_token}"
        return headers

    def handle_unauth(self, e: requests.RequestException, api_name: str):
        if e.response is not None and e.response.status_code == 401 and self.sa is None:
            print(
                f" ❌  Error calling {api_name} API. Unauthorized and no service account provided. Make sure the environment has default credentials set up or provide a service account name as SA_NAME in teams.py."
            )
        elif e.response is not None and e.response.status_code == 401 and self.sa is not None:
            print(
                f" ❌  Error calling {api_name} API. Unauthorized with provided service account: {self.sa}. Make sure the service account has the 'iap.webServiceVersions.accessViaIap' permission."
            )

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
            diff_response = response.json()
            return diff_response["diff"]
        except requests.RequestException as e:
            self.handle_unauth(e, "diff")
            print(f" ❌ Error calling diff API: {e}")
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
            print(f" ❌ Error calling upload API: {e}")
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
            print(f" ❌ Error deploying schedule: {e}")
            raise e

    def call_cancel_api(self, workflow_id):
        url = f"{self.base_url}/workflow/v2/{workflow_id}/cancel"

        try:
            response = requests.post(url, headers=self.auth_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "workflow cancel")
            print(f" ❌ Error calling workflow cancel API: {e}")
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
            print(f" ❌ Error calling sync API: {e}")
            raise e

    def call_eval_api(
        self,
        conf_name,
        conf_hash_map,
    ):
        if not self.eval_url:
            raise ValueError(" ❌ Eval URL not specified. Please specify EVAL_URL in teams.py, environment variables, or use the --eval-url flag.")
        _request = {
            "confName": conf_name,
            "confHashMap": conf_hash_map,
        }
        try:
            response = requests.post(self.eval_url + "/eval", json=_request, headers=self.auth_headers(self.eval_url))
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.handle_unauth(e, "eval")
            print(f" ❌ Error calling eval API: {e}")
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
            print(f" ❌ Error calling workflow start API: {e}")
            raise e
