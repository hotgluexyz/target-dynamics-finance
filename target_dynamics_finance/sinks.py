"""DynamicsFinance target sink class, which handles writing streams."""


from target_dynamics_finance.client import DynamicsSink


class InvoicesSink(DynamicsSink):
    """Dynamics-bc-onprem target sink class."""

    allowed_endpoints = {
        "VendorInvoiceHeaders": {
            "lines_endpoint": "VendorInvoiceLines",
            "primary_keys": ["dataAreaId", "HeaderReference"],
            "external_ref": "InvoiceNumber",
        }
    }
    available_names = ["VendorInvoiceHeaders"]

    @property
    def name(self):
        return self.stream_name

    @property
    def endpoint(self):
        return f"/{self.stream_name}"
    
    @property
    def invoice_values(self):
        return self.allowed_endpoints.get(self.name)
    
    @property
    def primary_key(self):
        return self.invoice_values.get("primary_keys")[-1]

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        for key, value in record.items():
            record[key] = self.clean_data(value)
        return record

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        headers = {}

        if record:
            lines = record.pop(self.invoice_values.get("lines_endpoint"), None)
            res = self.request_api(
                method, endpoint=self.endpoint, request_data=record, headers=headers
            )
            res = res.json()
            res_id = res.get(self.primary_key)
            if res_id:
                method = "POST"
                for line in lines:
                    lines_endpoint = f"/{self.invoice_values.get('lines_endpoint')}"
                    res = self.request_api(
                        method, endpoint=lines_endpoint, request_data=line, headers=headers
                    )
            return res_id, True, state_updates


class FallbackSink(DynamicsSink):
    """Dynamics-bc-onprem target sink class."""

    @property
    def name(self):
        return self.stream_name
    
    @property
    def endpoint(self):
        return f"/{self.stream_name}"

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        for key, value in record.items():
            record[key] = self.clean_data(value)
        return record

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if record:
            method = "POST"
            endpoint = self.endpoint
            headers = {}
            primary_key = self.key_properties[-1] if self.key_properties else None

            res = self.request_api(
                method, endpoint=endpoint, request_data=record, headers=headers
            )
            res = res.json()
            res_id = res.get(primary_key)
            return res_id, True, state_updates
