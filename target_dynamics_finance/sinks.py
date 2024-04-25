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
            if method == "PATCH":
                # IF we PATCHED the invoice header we are ignoring lines and attachments
                return res_id, True, state_updates

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

            return str(res_id), True, state_updates


class FallbackSink(DynamicsSink):
    """Dynamics-bc-onprem target sink class."""

    @property
    def name(self):
        return self.stream_name

    @property
    def endpoint(self):
        return f"/{self.stream_name}"
    
    lookup_keys = {
        "VendorsV3": "VendorAccountNumber"
    }

    not_send_fields_patch = {
        "VendorsV3": ["VendorGroupId", "TaxExemptNumber"]
    }

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        for key, value in record.items():
            record[key] = self.clean_data(value)
        return record

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        endpoint = self.endpoint
        if record:
            # set initial variables
            method = "POST"
            headers = {}
            params = {}
            primary_key = self.key_properties[-1] if self.key_properties else None

            # if lookup key available, do a lookup to patch
            lookup_key = self.lookup_keys.get(self.name)
            primary_keys = self.key_properties or []

            # check if there is an id for patching
            record_id = record.pop("id", None)
            existing_record = {}
            if record_id:
                existing_record = record.copy()
                existing_record[primary_key] = record_id
            # if no id lookup using lookup key
            elif lookup_key and record.get(lookup_key) and primary_keys:
                existing_record = None
                lookup_params = {"$filter": f"{lookup_key} eq '{record[lookup_key]}' and dataAreaId eq '{record['dataAreaId']}'"}
                existing_record = self.lookup(self.endpoint, lookup_params)

            # if there is an existing record do a PATCH
            if existing_record:
                method = "PATCH"
                identifier = self.get_unique_identifier(existing_record, primary_keys)
                endpoint = f"{self.endpoint}({identifier})"
                state_updates["is_updated"] = True
                params["cross-company"] = True
                res_id = existing_record[primary_key]

                # not send fields in not_send_fields_patch
                not_send_fields = self.not_send_fields_patch.get(self.name)
                if not_send_fields:
                    for field in not_send_fields:
                        record.pop(field, None)
            
            else:
                # primary key is set by dynamics, if this is a new record don't send the primary key value
                record.pop(primary_key, None)
                    
            res = self.request_api(
                method, endpoint=endpoint, request_data=record, headers=headers, params=params
            )
            if res.status_code != 204:
                res = res.json()
                res_id = res.get(primary_key)
            return str(res_id), True, state_updates
