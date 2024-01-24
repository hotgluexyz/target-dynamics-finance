from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from target_dynamics_finance.auth import DynamicsAuthenticator
import ast
import json
import datetime


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
