"""Short-lived Spark session utilities for educational notebooks."""

from __future__ import annotations

from types import TracebackType
from pyspark.sql import SparkSession

from lakehouse_lineage import OpenLineageContext


class SparkSessionContext:
    """Create one Spark session for a cell and always release its resources."""

    def __init__(
        self,
        app_name: str,
        *,
        log_level: str = "WARN",
        config: dict[str, str] | None = None,
        lineage_namespace: str = "jh",
    ) -> None:
        self.app_name = app_name
        self.log_level = log_level
        self.config = config or {}
        self.lineage_namespace = lineage_namespace
        self.spark: SparkSession | None = None

    def __enter__(self) -> SparkSessionContext:
        if self.spark is not None:
            raise RuntimeError("SparkSessionContext нельзя использовать повторно")
        builder = SparkSession.builder.appName(self.app_name)
        for key, value in self.config.items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.log_level)
        return self

    def __getattr__(self, name: str):
        """Delegate Spark operations while keeping lineage on the context."""
        spark = self.__dict__.get("spark")
        if spark is None:
            raise AttributeError(name)
        return getattr(spark, name)

    def lineage(self, job_name: str, *, inputs: list[str] | None = None, output: str | None = None, outputs: list[str] | None = None, description: str | None = None, dataset_descriptions=None, columns=None, config=None, input_facets=None, output_facets=None, job_facets=None, run_facets=None) -> OpenLineageContext:
        return OpenLineageContext(
            job_name,
            inputs=inputs,
            outputs=outputs or ([output] if output else None),
            namespace=self.lineage_namespace,
            spark=self.spark,
            description=description,
            dataset_descriptions=dataset_descriptions,
            column_lineage=columns,
            config=config,
            input_facets=input_facets,
            output_facets=output_facets,
            job_facets=job_facets,
            run_facets=run_facets,
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        try:
            if self.spark is not None:
                self.spark.stop()
        finally:
            self.spark = None
        return False
