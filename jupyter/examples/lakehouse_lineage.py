"""Small OpenLineage helper for interactive Jupyter work."""

from __future__ import annotations

import json
import os
import re
import socket
import urllib.request
import uuid
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Mapping


Facets = Mapping[str, Mapping[str, Any]]
PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/1.50.0"


def documentation_facet(description: str, *, job: bool = False) -> dict[str, Any]:
    """Build a standard OpenLineage documentation facet."""
    facet_type = "DocumentationJobFacet" if job else "DocumentationDatasetFacet"
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"https://openlineage.io/spec/facets/1-0-0/{facet_type}.json",
        "description": description,
    }


def iceberg_dataset_facets(
    spark,
    table_name: str,
    *,
    description: str | None = None,
    source_name: str = "lakehouse",
    source_uri: str = "http://iceberg-rest:8181",
) -> dict[str, Any]:
    """Collect the Iceberg specification exposed by DESCRIBE TABLE EXTENDED."""
    identifiers = table_name.split(".")
    if not identifiers or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item) for item in identifiers):
        raise ValueError(f"Invalid Iceberg table name: {table_name}")
    table_sql = ".".join(f"`{item}`" for item in identifiers)
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table_sql}").collect()
    fields = []
    details: dict[str, str] = {}
    section = "columns"
    for row in rows:
        name = str(row[0] or "").strip()
        value = str(row[1] or "").strip()
        comment = str(row[2] or "").strip()
        if name == "# Detailed Table Information":
            section = "details"
            continue
        if name.startswith("#"):
            section = "other"
            continue
        if not name:
            continue
        if section == "details":
            details[name] = value
            continue
        if section != "columns":
            continue
        item = {
            "name": name,
            "type": value,
            "description": comment or f"Поле {name} таблицы {table_name}.",
        }
        fields.append(item)

    provider = details.get("Provider", source_name)
    location = details.get("Location", source_uri)
    table_comment = details.get("Comment")
    description = description or table_comment or f"Таблица {table_name} ({provider}), расположение: {location}."

    facets: dict[str, Any] = {
        "schema": {
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json",
            "fields": fields,
        },
        "dataSource": {
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataSourceDatasetFacet.json",
            "name": provider,
            "uri": location,
        },
    }
    if description:
        facets["documentation"] = documentation_facet(description)
    return facets


def column_lineage_facet(
    output_columns: Mapping[str, list[tuple[str, str] | tuple[str, str, str]]],
    *,
    namespace: str = "jh",
) -> dict[str, Any]:
    """Map output columns to dataset fields, optionally with explicit namespaces."""
    return {
        "_producer": PRODUCER,
        "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
        "fields": {
            output: {
                "inputFields": [
                    {
                        "namespace": item[0] if len(item) == 3 else namespace,
                        "name": item[-2],
                        "field": item[-1],
                    }
                    for item in inputs
                ]
            }
            for output, inputs in output_columns.items()
        },
    }


class OpenLineageContext:
    """Emit a complete START/COMPLETE or START/FAIL run to Marquez."""

    def __init__(
        self,
        job_name: str,
        *,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        namespace: str = "jh",
        parent_run_id: str | None = None,
        input_facets: Mapping[str, Facets] | None = None,
        output_facets: Mapping[str, Facets] | None = None,
        job_facets: Facets | None = None,
        run_facets: Facets | None = None,
        spark=None,
        description: str | None = None,
        dataset_descriptions: Mapping[str, str] | None = None,
        column_lineage: Mapping[str, str | list[str]] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self.job_name = job_name
        self.inputs = inputs or []
        self.outputs = outputs or []
        self.namespace = namespace
        self.parent_run_id = parent_run_id
        self.input_facets = input_facets or {}
        self.output_facets = output_facets or {}
        self.job_facets = dict(job_facets or {})
        self.run_facets = dict(run_facets or {})
        self.spark = spark
        self.description = description
        self.dataset_descriptions = dataset_descriptions or {}
        self.column_lineage = column_lineage or {}
        self.config = config or {}
        self.run_id = str(uuid.uuid4())
        self.url = os.getenv("MARQUEZ_URL", "http://marquez:5000").rstrip("/")

    def _automatic_dataset_facets(self, name: str) -> dict[str, Any]:
        if self.spark is None:
            return {}
        table_name = name if name.count(".") >= 2 else f"lakehouse.{name}"
        try:
            return iceberg_dataset_facets(
                self.spark,
                table_name,
                description=self.dataset_descriptions.get(name),
            )
        except Exception:
            return {}

    def _dataset_id(self, name: str) -> tuple[str, str]:
        if name.startswith("raw."):
            return "lakehouse_raw", f"lakehouse.{name}"
        if name.startswith("stage."):
            return "jh_stage", name
        if name.startswith("mart."):
            return "jh_mart", name
        return self.namespace, name

    def _job_namespace(self) -> str:
        datasets = self.outputs or self.inputs
        if datasets:
            return self._dataset_id(datasets[0])[0]
        return self.namespace

    def _dataset(
        self,
        name: str,
        facets: Facets | None = None,
        *,
        output: bool = False,
        read_schema: bool = True,
    ) -> dict[str, Any]:
        dataset_namespace, dataset_name = self._dataset_id(name)
        dataset = {"namespace": dataset_namespace, "name": dataset_name}
        combined = self._automatic_dataset_facets(name) if read_schema else {}
        combined.update({key: dict(value) for key, value in (facets or {}).items()})
        if output and self.column_lineage:
            parsed: dict[str, list[tuple[str, str, str]]] = {}
            for column, sources in self.column_lineage.items():
                parsed[column] = []
                for source in ([sources] if isinstance(sources, str) else sources):
                    source_dataset, source_column = source.rsplit(".", 1)
                    source_namespace, source_name = self._dataset_id(source_dataset)
                    parsed[column].append((source_namespace, source_name, source_column))
            combined["columnLineage"] = column_lineage_facet(parsed)
        if combined:
            dataset["facets"] = combined
        return dataset

    def _post(self, event: Mapping[str, Any]) -> None:
        request = urllib.request.Request(
            f"{self.url}/api/v1/lineage",
            data=json.dumps(event).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=5).close()
        except Exception as send_error:
            print(f"OpenLineage event was not sent: {send_error}", flush=True)

    def _send(self, event_type: str, *, error: BaseException | None = None) -> None:
        job_facets = dict(self.job_facets)
        if self.description:
            job_facets.setdefault("documentation", documentation_facet(self.description, job=True))
        if self.config:
            configuration = "\n".join(f"- {key}: {value}" for key, value in self.config.items())
            current = self.description or self.job_name
            job_facets["documentation"] = documentation_facet(
                f"{current}\n\nConfiguration:\n{configuration}", job=True
            )
        event: dict[str, Any] = {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": self.run_id, 
                    "facets": {
                        "hostname": {
                        "_producer": "lakehouse-jupyter", 
                        "value": socket.gethostname()
                    },
                    **self.run_facets,
                }
            },
            "job": {
                "namespace": self._job_namespace(),
                "name": self.job_name, 
                "facets": job_facets,
            },
            "inputs": [self._dataset(name, self.input_facets.get(name)) for name in self.inputs],
            "outputs": [
                self._dataset(
                    name,
                    self.output_facets.get(name),
                    output=True,
                    read_schema=event_type == "COMPLETE",
                )
                for name in self.outputs
            ],
            "producer": PRODUCER,
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        }
        if self.parent_run_id:
            event["run"]["facets"]["parent"] = {
                "_producer": "lakehouse-jupyter", 
                "run": {
                    "runId": self.parent_run_id
                }
            }
        if error:
            event["run"]["facets"]["errorMessage"] = {
                "_producer": "lakehouse-jupyter", 
                "message": str(error)
            }
        self._post(event)

    def __enter__(self) -> "OpenLineageContext":
        self._send("START")
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, 
                traceback: TracebackType | None) -> bool:
        self._send("FAIL" if exc_value else "COMPLETE", error=exc_value)
        return False


class TableArtifact:
    """Describe an Iceberg table as an OpenLineage dataset artifact."""

    def __init__(self, table_name: str, *, namespace: str = "jh") -> None:
        self.table_name = table_name
        self.namespace = namespace

    @property
    def name(self) -> str:
        return f"lakehouse.{self.table_name}"

    def create(self, spark, schema: str, *, location: str | None = None) -> None:
        sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({schema}) USING iceberg"
        if location:
            sql += f" LOCATION '{location}'"
        spark.sql(sql)

    def __str__(self) -> str:
        return self.name
