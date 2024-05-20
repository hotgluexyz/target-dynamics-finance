from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from target_dynamics_finance.auth import DynamicsAuthenticator
import ast
import json
import datetime
import backoff
import requests
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError


class DynamicsSink(HotglueSink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    auth_state = {}
    available_names = []
    skip_record_patching = False

    @property
    def base_url(self) -> str:
        base_url = f"https://{self.config['subdomain']}.operations.dynamics.com/data"
        return base_url

    @property
    def authenticator(self):
        url = f"https://login.microsoftonline.com/{self.config.get('tenant')}/oauth2/token"
        return DynamicsAuthenticator(self._target, self.auth_state, url)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        return headers

    def parse_objs(self, obj):
        try:
            try:
                return ast.literal_eval(obj)
            except:
                return json.loads(obj)
        except:
            return obj

    def convert_date(self, date):
        if date:
            date = datetime.datetime.strptime(date)
            date = date.strftime("%Y-%m-%d")
            return date

    def clean_data(self, value):
        try:
            value = self.convert_date(value)
        except:
            if isinstance(value, str) and (
                value.startswith("[") or value.startswith("{")
            ):
                value = self.parse_objs(value)
        return value

    def lookup(self, endpoint, params):
        self.logger.info(f"Look up to {endpoint} filtering by {params}")
        params.update({"cross-company": True})
        res_id = self.request_api("GET", endpoint, params)
        res_id = res_id.json().get("value", [])
        if res_id:
            return res_id[0]
        
    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = self.http_headers
        
        # log request 
        if request_data and "FileContents" in request_data:
            self.logger.info(f"Sending request {http_method} to url {url} with params {params}")
        else:
            self.logger.info(f"Sending request {http_method} to url {url} with params {params} and body {request_data}")

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )
        val_resp = self.validate_response(response)
        # if note in validate_response return it to update the state
        if val_resp and "note" in val_resp:
            return val_resp
        return response
    
    def get_unique_identifier(self, object, primary_keys):
        identifier = []
        for pk in primary_keys:
            identifier.append(f"{pk}='{object[pk]}'")

        identifier = ",".join(identifier)
        return identifier
    
    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        # skip patching records only if record no longer exists in Dynamics Finance
        if response.status_code in [400]:
            if "No resources were found when selecting for update." in response.text:
                self.logger.info(f"Skipping record patching because {self.name} record was not found")
                self.skip_record_patching = True
                return {"note": f"Skipping record patching because {self.name} record was not found"}
        # apply standard logic to validate response
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            try:
                msg = response.text
            except:
                msg = self.response_error_message(response)
            raise FatalAPIError(msg)
