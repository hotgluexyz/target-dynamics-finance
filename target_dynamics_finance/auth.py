import json
from datetime import datetime
from typing import Optional
from typing import Any, Dict, Optional

import logging
import requests


class DynamicsAuthenticator:
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        target,
        state,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = auth_endpoint
        self._config_file = target.config_file
        self._target = target
        self.state = state

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._config.get('access_token')}"
        return result

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        if self._config.get("refresh_token"):
            return {
                "client_id": self._config["client_id"],
                "client_secret": self._config["client_secret"],
                "redirect_uri": self._config["redirect_uri"],
                "refresh_token": self._config["refresh_token"],
                "grant_type": "refresh_token",
            }
        else:
            # get subdomain
            if self._config.get("subdomain"):
                subdomain = self._config["subdomain"]
            else:
                subdomain = self._config.get("base_url").replace("https://", "").replace(".operations.dynamics.com", "")

            # return payload 
            return {
                "resource": f"https://{subdomain}.operations.dynamics.com",
                "grant_type": "client_credentials",
                "client_id": str(self._config["client_id"]),
                "client_secret": str(self._config["client_secret"]),
            }
        

    def is_token_valid(self) -> bool:
        access_token = self._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._config.get("expires_in")
        if expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False
        if not expires_in:
            return False
        return not ((expires_in - now) < 120)

    def update_access_token(self) -> None:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        self.logger.info(
            f"Oauth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}"
        )
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body, headers=headers
        )

        if token_response.status_code not in [200]:
            raise Exception(f"Authentication error status code {token_response.status_code} - {token_response.text}")

        token_json = token_response.json()
        self.access_token = token_json["access_token"]

        self._config["access_token"] = token_json["access_token"]
        self._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._config["expires_in"] = int(token_json["expires_in"]) + now

        with open(self._target.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)
