"""PySpark DataSource implementation for SilentPulse.

Provides a custom Spark DataSource that reads visibility status,
alert data, and flow health from a SilentPulse instance.

Usage::

    spark.dataSource.register(SilentPulseDataSource)

    df = (
        spark.read
        .format("silentpulse")
        .option("url", "https://silentpulse.example.com")
        .option("token", dbutils.secrets.get("silentpulse", "pat"))
        .option("resource", "alerts")
        .load()
    )
"""

from __future__ import annotations

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceStreamWriter,
    InputPartition,
)

# -- Schemas (DDL) per resource -------------------------------------------------

SCHEMAS = {
    "alerts": (
        "id STRING, flow_pulse_id STRING, asset_id STRING, status STRING, "
        "alert_type STRING, severity STRING, started_at TIMESTAMP, "
        "resolved_at TIMESTAMP, tenant_id STRING, asset_identifier STRING, "
        "pulse_label STRING, flow_name STRING, asset_group_name STRING"
    ),
    "flows": (
        "id STRING, name STRING, description STRING, enabled BOOLEAN, "
        "asset_group_id STRING, tenant_id STRING, "
        "created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
    "entities": (
        "id STRING, observation_type_id STRING, external_id STRING, "
        "traits STRING, tenant_id STRING, "
        "created_at TIMESTAMP, updated_at TIMESTAMP"
    ),
    "observations": (
        "id STRING, name STRING, slug STRING, description STRING, icon STRING, entity_count INT, group_count INT"
    ),
    "coverage": (
        "flow_id STRING, flow_name STRING, flow_enabled BOOLEAN, "
        "asset_group_name STRING, total_assets INT, stages STRING"
    ),
}

# Resource -> (API path, paginated?, response key)
RESOURCE_ENDPOINTS = {
    "alerts": ("/api/v1/alerts", True, "data"),
    "flows": ("/api/v1/flows", True, "data"),
    "entities": ("/api/v1/observed-entities", True, "data"),
    "observations": ("/api/v1/observation-types", True, "data"),
    "coverage": ("/api/v1/dashboard/pipeline-coverage", False, "pipelines"),
}


# -- Partitions -----------------------------------------------------------------


class SimplePartition(InputPartition):
    """Single partition for sequential REST API reads."""

    def __init__(self, partition_id):
        self.partition_id = partition_id


class TimeRangePartition(InputPartition):
    """Partition bounded by start/end ISO timestamps."""

    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time


# -- DataSource -----------------------------------------------------------------


class SilentPulseDataSource(DataSource):
    """Spark DataSource for reading SilentPulse data.

    Options:
        url (required): SilentPulse API base URL.
        token (required): Personal Access Token (``sp_pat_...``).
        resource: ``alerts`` | ``flows`` | ``entities`` | ``observations`` | ``coverage``.
        page_size: Batch pagination size (default 100).
        timeout: Request timeout in seconds (default 30).
        max_retries: Retry count for transient errors (default 3).
    """

    @classmethod
    def name(cls):
        return "silentpulse"

    def schema(self):
        resource = self.options.get("resource", "alerts")
        if resource not in SCHEMAS:
            raise ValueError(f"Unknown resource '{resource}'. Valid resources: {', '.join(sorted(SCHEMAS))}")
        return SCHEMAS[resource]

    def reader(self, schema):
        return SilentPulseBatchReader(self.options, schema)

    def streamReader(self, schema):
        resource = self.options.get("resource", "alerts")
        if resource != "alerts":
            raise ValueError(f"Streaming read is only supported for 'alerts' resource, got '{resource}'")
        return SilentPulseStreamReader(self.options, schema)

    def streamWriter(self, schema, overwrite):
        return SilentPulseStreamWriter(self.options, schema)


# -- Base Reader ----------------------------------------------------------------


class SilentPulseReader:
    """Shared logic for batch and stream readers."""

    def __init__(self, options, schema):
        self.url = options.get("url", "").rstrip("/")
        assert self.url, "url option is required"
        self.token = options.get("token", "")
        assert self.token, "token option is required"
        self.resource = options.get("resource", "alerts")
        self.page_size = int(options.get("page_size", "100"))
        self.timeout = int(options.get("timeout", "30"))
        self.max_retries = int(options.get("max_retries", "3"))
        self._schema = schema

    def _get_field_names(self):
        """Extract ordered field names from schema (StructType or DDL string)."""
        from pyspark.sql.types import StructType

        if isinstance(self._schema, StructType):
            return [f.name for f in self._schema.fields]
        fields = []
        for part in self._schema.split(","):
            name = part.strip().split()[0]
            fields.append(name)
        return fields

    def _fetch_page(self, session, page, extra_params=None):
        """Fetch a single page from the API with retry on transient errors."""
        import time

        endpoint, paginated, response_key = RESOURCE_ENDPOINTS[self.resource]
        api_url = f"{self.url}{endpoint}"

        params = {}
        if paginated:
            params["page"] = page
            params["page_size"] = self.page_size
        if extra_params:
            params.update(extra_params)

        headers = {"Authorization": f"Bearer {self.token}"}

        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = session.get(api_url, params=params, headers=headers, timeout=self.timeout)

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    delay = int(retry_after) if retry_after else (2**attempt)
                    time.sleep(delay)
                    continue

                if 400 <= resp.status_code < 500:
                    resp.raise_for_status()

                if resp.status_code >= 500:
                    last_exc = Exception(f"Server error {resp.status_code}: {resp.text[:200]}")
                    time.sleep(2**attempt)
                    continue

                body = resp.json()
                items = body.get(response_key, []) if isinstance(body, dict) else body
                total_pages = body.get("total_pages", 1) if isinstance(body, dict) else 1
                return items, total_pages

            except Exception as exc:
                if attempt >= self.max_retries:
                    raise
                last_exc = exc
                time.sleep(2**attempt)

        raise last_exc  # pragma: no cover

    def _row_to_tuple(self, item, field_names):
        """Convert a JSON dict to a tuple in schema column order."""
        import json

        values = []
        for name in field_names:
            val = item.get(name)
            if isinstance(val, (dict, list)):
                val = json.dumps(val)
            values.append(val)
        return tuple(values)

    def _read_all_pages(self, session, extra_params=None):
        """Paginate through all pages and yield tuples."""
        field_names = self._get_field_names()
        _, paginated, _ = RESOURCE_ENDPOINTS[self.resource]

        if not paginated:
            items, _ = self._fetch_page(session, 1, extra_params)
            for item in items:
                yield self._row_to_tuple(item, field_names)
            return

        page = 1
        while True:
            items, total_pages = self._fetch_page(session, page, extra_params)
            for item in items:
                yield self._row_to_tuple(item, field_names)
            if page >= total_pages or not items:
                break
            page += 1


# -- Batch Reader ---------------------------------------------------------------


class SilentPulseBatchReader(SilentPulseReader, DataSourceReader):
    """Reads all data for a resource via paginated REST API calls."""

    def partitions(self):
        return [SimplePartition(0)]

    def read(self, partition):
        import requests

        with requests.Session() as session:
            yield from self._read_all_pages(session)


# -- Stream Reader (alerts only) ------------------------------------------------


class SilentPulseStreamReader(SilentPulseReader, DataSourceStreamReader):
    """Incrementally reads new alerts via started_after / started_before filters."""

    def initialOffset(self):
        import json

        return json.dumps({"timestamp": "1970-01-01T00:00:00Z"})

    def latestOffset(self):
        import json
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return json.dumps({"timestamp": now})

    def partitions(self, start, end):
        import json

        start_ts = json.loads(start)["timestamp"]
        end_ts = json.loads(end)["timestamp"]
        return [TimeRangePartition(start_ts, end_ts)]

    def read(self, partition):
        import requests

        extra_params = {
            "started_after": partition.start_time,
            "started_before": partition.end_time,
        }
        with requests.Session() as session:
            yield from self._read_all_pages(session, extra_params)

    def commit(self, end):
        pass


# -- Base Writer ----------------------------------------------------------------


class SilentPulseWriter:
    """Shared logic for stream writing to telemetry ingest endpoint."""

    def __init__(self, options, schema):
        self.url = options.get("url", "").rstrip("/")
        assert self.url, "url option is required"
        self.token = options.get("token", "")
        assert self.token, "token option is required"
        self.timeout = int(options.get("timeout", "30"))
        self.max_retries = int(options.get("max_retries", "3"))
        self.batch_size = int(options.get("batch_size", "50"))
        self._schema = schema

    def write(self, iterator):
        import time

        import requests

        api_url = f"{self.url}/api/v1/telemetry/ingest"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        batch = []
        count = 0

        for row in iterator:
            count += 1
            batch.append(row.asDict())

            if len(batch) >= self.batch_size:
                self._send_batch(requests, api_url, headers, batch, time)
                batch = []

        if batch:
            self._send_batch(requests, api_url, headers, batch, time)

        from pyspark.sql.datasource import WriterCommitMessage

        return WriterCommitMessage()

    def _send_batch(self, requests_mod, api_url, headers, events, time_mod):
        """POST a batch of events with retry on transient errors."""
        payload = {"events": events}

        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = requests_mod.post(api_url, json=payload, headers=headers, timeout=self.timeout)
                if resp.status_code < 400:
                    return
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    delay = int(retry_after) if retry_after else (2**attempt)
                    time_mod.sleep(delay)
                    continue
                if resp.status_code >= 500:
                    last_exc = Exception(f"Server error {resp.status_code}")
                    time_mod.sleep(2**attempt)
                    continue
                resp.raise_for_status()
            except Exception as exc:
                if attempt >= self.max_retries:
                    raise
                last_exc = exc
                time_mod.sleep(2**attempt)

        if last_exc:
            raise last_exc


# -- Stream Writer --------------------------------------------------------------


class SilentPulseStreamWriter(SilentPulseWriter, DataSourceStreamWriter):
    """Writes telemetry events to SilentPulse ingest endpoint."""

    def commit(self, messages, batchId):
        pass

    def abort(self, messages, batchId):
        pass
