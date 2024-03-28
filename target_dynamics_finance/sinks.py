"""DynamicsFinance target sink class, which handles writing streams."""


from target_dynamics_finance.client import DynamicsSink
import base64


class InvoicesSink(DynamicsSink):
    """Dynamics-bc-onprem target sink class."""

    allowed_endpoints = {
        "VendorInvoiceHeaders": {
            "lines_endpoint": "VendorInvoiceLines",
            "attachments_endpoint": "VendorInvoiceDocumentAttachments",
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
    
    def get_attachment_payload(self, payload, reference_id):
        input_path = self.config.get("input_path",'./')
        input_path = f"{input_path}/" if not input_path.endswith("/") else input_path
        attachment_id = payload.pop("Id", "")
        attachment_name = payload.get('Name')
        if attachment_id:
            attachment_name = f"{attachment_id}_{attachment_name}"
        
        with open(f"{input_path}{attachment_name}", "rb") as f:
            attachment = f.read()
            attachment = base64.b64encode(attachment)
        
        payload["FileContents"] = attachment
        payload["HeaderReference"] = reference_id
        return payload

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        headers = {}
        endpoint = self.endpoint
        params = {}

        if record:
            lines = record.pop(self.invoice_values.get("lines_endpoint"), None)
            attachments = record.pop("attachments") or []
            # lookup supplier
            vendor_account = self.lookup(
                "/VendorsV3",
                {
                    "$filter": f"VendorOrganizationName eq '{record.get('VendorName')}' and dataAreaId eq '{record.get('dataAreaId')}'"
                },
            )
            if vendor_account:
                vendor_account = vendor_account.get("VendorAccountNumber")
                if vendor_account:
                    record["InvoiceAccount"] = vendor_account
                else:
                    raise Exception(
                        "Skipping line because vendor doesn't exist in Dynamics"
                    )
            
            id = record.pop("id", None)
            identifier = None
            if id:
                record[self.primary_key] = id
                method = "PATCH"
                identifier = self.get_unique_identifier(record, self.allowed_endpoints[self.name]["primary_keys"])
                endpoint = f"{self.endpoint}({identifier})"
                params={"cross-company": True}

            res = self.request_api(
                method, endpoint=endpoint, request_data=record, headers=headers, params=params
            )
            res = res.json()
            res_id = res.get(self.primary_key)
            if res_id:
                method = "POST"

                try:
                    for line in lines:
                        lines_endpoint = f"/{self.invoice_values.get('lines_endpoint')}"
                        line[self.primary_key] = res_id
                        res = self.request_api(
                            method,
                            endpoint=lines_endpoint,
                            request_data=line,
                            headers=headers,
                        )
                except Exception as e:
                    self.logger.info(f"Posting line {line} has failed")
                    self.logger.info("Deleting purchase /invoice header")
                    identifier = self.get_unique_identifier(res, self.allowed_endpoints[self.name]["primary_keys"])
                    delete_endpoint = f"{self.endpoint}({identifier})"
                    purchase_order_lines = self.request_api(
                        "DELETE", endpoint=delete_endpoint, params={"cross-company": True}
                    )
                    error = {
                        "error": e,
                        "notes": "due to error during posting lines the purchase invoice header was deleted",
                    }
                    raise Exception(error)
                
                for attachment in attachments:
                    payload = self.get_attachment_payload(attachment, res_id)
                    attachments_endpoint = f"/{self.invoice_values.get('attachments_endpoint')}"
                    res = self.request_api(
                        method, endpoint=attachments_endpoint, request_data=payload, headers=headers
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
