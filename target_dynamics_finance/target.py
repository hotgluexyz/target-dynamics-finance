"""DynamicsFinance target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue
from typing import List, Optional, Union
from pathlib import PurePath

from target_dynamics_finance.sinks import FallbackSink, InvoicesSink


class TargetDynamicsFinance(TargetHotglue):
    """Sample target for DynamicsFinance."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)

    name = "target-dynamics-finance"
    SINK_TYPES = [FallbackSink, InvoicesSink]

    config_jsonschema = th.PropertiesList(
        th.Property("subdomain", th.StringType, required=True),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("tenant", th.StringType, required=True),
    ).to_dict()

    def get_sink_class(self, stream_name: str):
        for sink_class in self.SINK_TYPES:
            # Search for streams with multiple names
            if stream_name in sink_class.available_names:
                return sink_class

        # Adds a fallback sink for streams that are not supported
        return FallbackSink


if __name__ == "__main__":
    TargetDynamicsFinance.cli()
