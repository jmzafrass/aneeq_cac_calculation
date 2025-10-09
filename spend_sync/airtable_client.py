import math
from typing import Dict, Iterable, List, Optional

import requests

AIRTABLE_API_URL = "https://api.airtable.com/v0"


class AirtableError(RuntimeError):
    pass


class AirtableClient:
    def __init__(self, api_key: str, base_id: str):
        self.api_key = api_key
        self.base_id = base_id

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, table: str, **kwargs) -> Dict:
        url = f"{AIRTABLE_API_URL}/{self.base_id}/{table}"
        response = requests.request(method, url, headers=self.headers, **kwargs)
        if response.status_code >= 400:
            raise AirtableError(f"{method} {url} failed: {response.status_code} {response.text}")
        return response.json()

    def iter_records(self, table: str, fields: Optional[List[str]] = None, filter_formula: Optional[str] = None):
        params: Dict[str, object] = {}
        if fields:
            params["fields[]"] = fields
        if filter_formula:
            params["filterByFormula"] = filter_formula

        offset = None
        while True:
            if offset:
                params["offset"] = offset
            else:
                params.pop("offset", None)
            payload = self._request("GET", table, params=params)
            for record in payload.get("records", []):
                yield record
            offset = payload.get("offset")
            if not offset:
                break

    def update_records(self, table: str, records: List[Dict], chunk_size: int = 10) -> None:
        for i in range(0, len(records), chunk_size):
            batch = {"records": records[i : i + chunk_size]}
            self._request("PATCH", table, json=batch)

    def create_records(self, table: str, records: List[Dict], chunk_size: int = 10) -> None:
        for i in range(0, len(records), chunk_size):
            batch = {"records": records[i : i + chunk_size]}
            self._request("POST", table, json=batch)

    def upsert_by_id(self, table: str, records: Iterable[Dict]) -> None:
        existing = {}
        for record in self.iter_records(table, fields=["id"]):
            fields = record.get("fields", {})
            identifier = fields.get("id")
            if identifier:
                existing[str(identifier)] = record["id"]

        updates: List[Dict] = []
        creates: List[Dict] = []
        for payload in records:
            identifier = payload["fields"].get("id")
            airtable_id = existing.get(str(identifier))
            if airtable_id:
                updates.append({"id": airtable_id, "fields": payload["fields"]})
            else:
                creates.append(payload)

        if updates:
            self.update_records(table, updates)
        if creates:
            self.create_records(table, creates)

    def update_single_record(self, table: str, record_id: str, fields: Dict[str, object]) -> None:
        self._request("PATCH", table, json={"records": [{"id": record_id, "fields": fields}]})

    def get_single_record(self, table: str, metric_name: str) -> Dict:
        records = list(self.iter_records(table, filter_formula=f"{{Metric}} = '{metric_name}'"))
        if not records:
            raise AirtableError(f"Metric '{metric_name}' not found in {table}")
        return records[0]
