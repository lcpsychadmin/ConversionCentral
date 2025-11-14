from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha1
from typing import Callable, Iterator, Mapping, MutableMapping, Sequence, TYPE_CHECKING
from urllib.parse import quote

from sqlalchemy import Column, MetaData, Table, insert, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import sqltypes

from app.ingestion import get_ingestion_engine
from app.ingestion.engine import get_ingestion_connection_params


logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - imported lazily to avoid hard dependency on pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType


def _normalize_identifier(value: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value.strip())
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized or "column"


def _quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


@dataclass(frozen=True)
class LoadPlan:
    schema: str
    table_name: str
    replace: bool
    deduplicate: bool


class BaseTableLoader:
    """Shared ingestion loader behaviour for Databricks-backed targets."""

    def __init__(
        self,
        *,
        default_schema: str | None = None,
        batch_rows: int | None = None,
    ) -> None:
        self._default_schema_override = (
            default_schema.strip() if isinstance(default_schema, str) and default_schema.strip() else None
        )
        self._batch_rows_override = batch_rows if isinstance(batch_rows, int) and batch_rows > 0 else None
        self._default_schema = "default"
        self._default_catalog: str | None = None
        self._insert_batch_rows = 1000
        self._apply_connection_defaults()

    def load_rows(
        self,
        plan: LoadPlan,
        rows: Sequence[Mapping[str, object]],
        columns: Sequence[Column],
    ) -> int:
        raise NotImplementedError

    @property
    def default_schema(self) -> str:
        self._apply_connection_defaults()
        return self._default_schema

    @property
    def default_catalog(self) -> str | None:
        self._apply_connection_defaults()
        return self._default_catalog

    def _apply_connection_defaults(self) -> None:
        schema = self._default_schema_override
        batch_rows = self._batch_rows_override
        params = None
        try:
            params = get_ingestion_connection_params()
        except RuntimeError:
            params = None

        if schema is None and params:
            candidate = params.schema_name or params.constructed_schema
            if candidate:
                schema = candidate.strip() or None
        if schema is None:
            schema = "default"
        if isinstance(schema, str):
            schema = schema.strip() or "default"

        if batch_rows is None and params and isinstance(params.ingestion_batch_rows, int):
            if params.ingestion_batch_rows > 0:
                batch_rows = params.ingestion_batch_rows
        if batch_rows is None:
            batch_rows = 1000

        catalog = None
        if params and getattr(params, "catalog", None):
            candidate_catalog = params.catalog.strip()
            catalog = candidate_catalog or None

        self._default_schema = schema
        self._insert_batch_rows = batch_rows
        self._default_catalog = catalog

    def _prepare_row(self, row: Mapping[str, object], deduplicate: bool) -> MutableMapping[str, object]:
        normalized: MutableMapping[str, object] = {}
        for key, value in row.items():
            normalized[self._normalize_identifier(key)] = value
        if deduplicate:
            digest = sha1()
            for key in sorted(normalized.keys()):
                if key == "__cc_row_hash":
                    continue
                digest.update(key.encode("utf-8"))
                digest.update(str(normalized[key]).encode("utf-8", errors="ignore"))
            normalized["__cc_row_hash"] = digest.hexdigest()
        return normalized

    def _normalize_identifier(self, value: str) -> str:
        return _normalize_identifier(value)


class DatabricksTableLoader(BaseTableLoader):
    """Lightweight loader that mirrors source rows into Databricks staging tables via SQL."""

    def __init__(
        self,
        *,
        engine: Engine | None = None,
        default_schema: str | None = None,
        batch_rows: int | None = None,
    ) -> None:
        self.engine = engine or get_ingestion_engine()
        super().__init__(default_schema=default_schema, batch_rows=batch_rows)

    def load_rows(
        self,
        plan: LoadPlan,
        rows: Sequence[Mapping[str, object]],
        columns: Sequence[Column],
    ) -> int:
        if not rows:
            return 0

        self._apply_connection_defaults()
        schema = plan.schema or self._default_schema
        table_name = self._normalize_identifier(plan.table_name)
        column_specs = self._infer_column_specs(columns, rows[0])
        target_table = self._ensure_table(schema, table_name, column_specs, plan.deduplicate)

        prepared_rows = [self._prepare_row(row, plan.deduplicate) for row in rows]

        with self.engine.begin() as connection:
            if plan.replace:
                qualified = self._qualify(schema, table_name)
                connection.execute(text(f"DELETE FROM {qualified}"))
            total = 0
            for chunk in self._iter_chunks(prepared_rows, self._insert_batch_rows):
                result = connection.execute(insert(target_table), chunk)
                total += result.rowcount or len(chunk)
        return total

    def _ensure_table(
        self,
        schema: str,
        table_name: str,
        column_specs: Mapping[str, sqltypes.TypeEngine],
        deduplicate: bool,
    ) -> Table:
        metadata = MetaData(schema=schema or None)
        columns = [Column(name, type_, nullable=True) for name, type_ in column_specs.items()]
        if deduplicate:
            columns.append(Column("__cc_row_hash", sqltypes.String(length=40), nullable=False))

        table = Table(table_name, metadata, *columns, extend_existing=True)
        self._ensure_schema_exists(schema)
        metadata.create_all(self.engine, tables=[table])
        return table

    @staticmethod
    def _iter_chunks(
        rows: Sequence[MutableMapping[str, object]],
        chunk_size: int,
    ) -> Iterator[Sequence[MutableMapping[str, object]]]:
        if chunk_size <= 0:
            chunk_size = len(rows)
        chunk_size = max(1, chunk_size)
        for index in range(0, len(rows), chunk_size):
            yield rows[index : index + chunk_size]

    def _infer_column_specs(
        self,
        columns: Sequence[Column],
        sample_row: Mapping[str, object],
    ) -> dict[str, sqltypes.TypeEngine]:
        specs: dict[str, sqltypes.TypeEngine] = {}
        for column in columns:
            normalized = self._normalize_identifier(column.name)
            specs[normalized] = self._map_column_type(column.type)
        for key, value in sample_row.items():
            normalized = self._normalize_identifier(key)
            specs.setdefault(normalized, self._map_python_type(value))
        if not specs:
            specs["__cc_placeholder"] = sqltypes.String()
        return specs

    def _map_column_type(self, type_: sqltypes.TypeEngine) -> sqltypes.TypeEngine:
        if isinstance(type_, sqltypes.Boolean):
            return sqltypes.Boolean()
        if isinstance(type_, (sqltypes.Integer, sqltypes.BigInteger, sqltypes.SmallInteger)):
            return sqltypes.BigInteger()
        if isinstance(type_, sqltypes.Numeric):
            precision = getattr(type_, "precision", None) or 38
            scale = getattr(type_, "scale", None) or 10
            return sqltypes.Numeric(precision=precision, scale=scale)
        if isinstance(type_, sqltypes.Float):
            return sqltypes.Float()
        if isinstance(type_, sqltypes.DateTime):
            return sqltypes.DateTime(timezone=True)
        if isinstance(type_, sqltypes.Date):
            return sqltypes.Date()
        if isinstance(type_, sqltypes.Time):
            return sqltypes.Time()
        if isinstance(type_, sqltypes.LargeBinary):
            return sqltypes.LargeBinary()
        length = getattr(type_, "length", None)
        if length and length <= 4000:
            return sqltypes.String(length=length)
        return sqltypes.String()

    def _map_python_type(self, value: object) -> sqltypes.TypeEngine:
        if isinstance(value, bool):
            return sqltypes.Boolean()
        if isinstance(value, int):
            return sqltypes.BigInteger()
        if isinstance(value, float):
            return sqltypes.Float()
        if hasattr(value, "tzinfo") or hasattr(value, "isoformat"):
            return sqltypes.DateTime(timezone=True)
        if value is None:
            return sqltypes.String()
        return sqltypes.String()

    def _qualify(self, schema: str, table: str) -> str:
        if schema:
            return f"`{schema}`.`{table}`"
        return f"`{table}`"

    def _ensure_schema_exists(self, schema: str | None) -> None:
        if not schema:
            return
        if self.engine.dialect.name == "sqlite":  # sqlite does not support CREATE SCHEMA
            return
        stmt = text(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
        with self.engine.begin() as connection:
            connection.execute(stmt)

    def _normalize_identifier(self, value: str) -> str:
        return _normalize_identifier(value)


class SparkTableLoader(BaseTableLoader):
    """Loader that batches rows through a remote Spark session using Databricks Connect."""

    def __init__(
        self,
        *,
        spark: "SparkSession" | None = None,
        session_factory: Callable[[], "SparkSession"] | None = None,
        default_schema: str | None = None,
        batch_rows: int | None = None,
    ) -> None:
        self._spark = spark
        self._session_factory = session_factory
        self._serverless_session_id: str | None = None
        super().__init__(default_schema=default_schema, batch_rows=batch_rows)

    @property
    def spark(self) -> "SparkSession":
        if self._spark is None:
            self._spark = self._create_session()
        return self._spark

    def load_rows(
        self,
        plan: LoadPlan,
        rows: Sequence[Mapping[str, object]],
        columns: Sequence[Column],
    ) -> int:
        if not rows:
            return 0

        last_error: Exception | None = None
        for attempt in range(2):
            try:
                self._apply_connection_defaults()
                schema_name = plan.schema or self._default_schema
                table_name = self._normalize_identifier(plan.table_name)
                prepared_rows = [self._prepare_row(row, plan.deduplicate) for row in rows]

                data_rows, dataframe_schema = self._build_dataframe_inputs(columns, prepared_rows)

                spark = self.spark
                self._ensure_schema_exists(schema_name)
                table_identifier_sql = self._compose_table_identifier(schema_name, table_name, quote=True)

                df = spark.createDataFrame(data_rows, schema=dataframe_schema)
                if plan.replace and self._table_exists(schema_name, table_name):
                    spark.sql(f"DELETE FROM {table_identifier_sql}")

                df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_identifier_sql)
                return len(prepared_rows)
            except Exception as exc:  # pragma: no cover - defensive fallback for remote failures
                last_error = exc
                if attempt == 0 and self._should_reset_session(exc):
                    logger.warning("Spark session expired; recreating and retrying load", exc_info=exc)
                    self._reset_spark_session(detach_serverless=True)
                    continue
                break

        if last_error:
            raise last_error
        return 0

    def _create_session(self) -> "SparkSession":
        if self._session_factory is not None:
            return self._session_factory()

        try:
            from databricks.connect import DatabricksSession
            from databricks.sdk.core import Config
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "Spark ingestion requires the 'databricks-connect' package. Install databricks-connect to enable Spark ingestion."
            ) from exc

        params = get_ingestion_connection_params()
        host = params.workspace_host
        if not host:
            raise RuntimeError("Spark ingestion requires a configured Databricks workspace host.")
        if not host.startswith("https://"):
            host = f"https://{host}"
        token = params.access_token
        if not token:
            raise RuntimeError("Spark ingestion requires a Databricks personal access token.")

        warehouse_id = self._extract_warehouse_id(params.http_path)
        if not warehouse_id:
            raise RuntimeError(
                "Unable to derive a Databricks warehouse identifier from the configured http_path; Spark ingestion currently supports SQL warehouses only."
            )

        http_path = (params.http_path or "").strip()
        warehouse_id = warehouse_id.strip() if isinstance(warehouse_id, str) else warehouse_id
        # Databricks Spark Connect requires an explicit compute header to disambiguate
        # classic clusters from serverless SQL warehouses.
        compute_mode = params.spark_compute or "classic"
        compute_mode = compute_mode.strip().lower()
        if compute_mode not in {"classic", "serverless"}:
            compute_mode = "classic"

        config_kwargs: dict[str, str] = {"host": host, "token": token}

        sanitized_headers: dict[str, str] = {}
        if compute_mode != "serverless":
            if http_path:
                config_kwargs["sql_http_path"] = http_path
            if warehouse_id:
                config_kwargs["cluster_id"] = warehouse_id

        config = Config(**config_kwargs)

        builder = DatabricksSession.Builder()
        builder.sdkConfig(config)
        builder.validateSession(False)
        if compute_mode == "serverless":
            builder._headers.pop("x-databricks-http-path", None)
            builder._headers.pop("x-databricks-warehouse-id", None)

        if compute_mode == "serverless" and warehouse_id:
            session_id = self._get_serverless_session_id(host, token, warehouse_id)
            builder.header("x-databricks-session-id", session_id)
            builder._headers = {"x-databricks-session-id": session_id}
            masked_id = session_id if len(session_id) <= 8 else f"{session_id[:4]}...{session_id[-4:]}"
            sanitized_headers["x-databricks-session-id"] = masked_id
        elif warehouse_id:
            sanitized_headers["x-databricks-cluster-id"] = warehouse_id

        sanitized_config = {
            key: ("***" if key == "token" else value)
            for key, value in config_kwargs.items()
        }

        logger.info(
            "Spark Connect configuration resolved",
            extra={
                "compute_mode": compute_mode,
                "config": sanitized_config,
                "headers": sanitized_headers,
            },
        )

        return builder.getOrCreate()

    def _should_reset_session(self, exc: Exception) -> bool:
        message = str(exc).lower()
        if "session_id is no longer usable" in message:
            return True
        if "inactivity_timeout" in message:
            return True
        if "invalid session" in message and "session" in message:
            return True
        return False

    def _reset_spark_session(self, *, detach_serverless: bool) -> None:
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:  # pragma: no cover - best effort cleanup
                pass
        if detach_serverless and self._serverless_session_id:
            self._delete_serverless_session(self._serverless_session_id)
        self._spark = None
        self._serverless_session_id = None

    def _delete_serverless_session(self, session_id: str) -> None:
        try:
            import requests
        except ModuleNotFoundError:  # pragma: no cover - requests is already required elsewhere
            return

        try:
            params = get_ingestion_connection_params()
        except RuntimeError:  # pragma: no cover - fallback if params unavailable
            return

        host = params.workspace_host or ""
        if not host:
            return
        base_url = host if host.startswith("http") else f"https://{host}"
        token = params.access_token
        if not token:
            return

        url = f"{base_url.rstrip('/')}/api/2.0/sql/sessions/{quote(session_id)}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.delete(url, headers=headers, timeout=15)
            if response.status_code not in {200, 204, 404}:
                logger.debug(
                    "Databricks session delete returned unexpected status",
                    extra={"status": response.status_code, "detail": response.text},
                )
        except requests.RequestException:  # pragma: no cover - network failures ignored
            logger.debug("Failed to delete Databricks session", exc_info=True)

    def _get_serverless_session_id(self, host: str, token: str, warehouse_id: str) -> str:
        try:
            import requests
        except ModuleNotFoundError as exc:  # pragma: no cover - requests is a core dependency
            raise RuntimeError(
                "Spark ingestion requires the 'requests' package to negotiate serverless sessions."
            ) from exc

        base_url = host if host.startswith("http") else f"https://{host}"
        url = f"{base_url.rstrip('/')}/api/2.0/sql/sessions"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"warehouse_id": warehouse_id, "channel": {"name": "spark_connect"}}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
        except requests.RequestException as exc:  # pragma: no cover - network failure
            raise RuntimeError("Failed to create Databricks serverless session.") from exc

        if response.status_code != 200:
            detail = response.text.strip() or response.reason
            raise RuntimeError(
                f"Databricks rejected the serverless session request ({response.status_code}): {detail}"
            )

        data = response.json()
        session_id = data.get("session_id")
        if not session_id:
            raise RuntimeError("Databricks did not return a serverless session identifier.")

        self._serverless_session_id = session_id
        masked_id = session_id if len(session_id) <= 8 else f"{session_id[:4]}...{session_id[-4:]}"
        logger.info(
            "Databricks serverless session established",
            extra={"warehouse_id": warehouse_id, "session_id": masked_id},
        )
        return session_id

    def _build_dataframe_inputs(
        self,
        columns: Sequence[Column],
        prepared_rows: Sequence[MutableMapping[str, object]],
    ) -> tuple[list[tuple[object, ...]], "StructType"]:
        import pyspark.sql.types as T  # pragma: no cover - imported lazily

        column_order: list[str] = []
        for column in columns:
            normalized = self._normalize_identifier(column.name)
            if normalized not in column_order:
                column_order.append(normalized)
        for row in prepared_rows:
            for key in row.keys():
                if key not in column_order:
                    column_order.append(key)

        data_rows = [tuple(row.get(name) for name in column_order) for row in prepared_rows]

        fields: list[T.StructField] = []
        for name in column_order:
            sqlalchemy_type = None
            for column in columns:
                if self._normalize_identifier(column.name) == name:
                    sqlalchemy_type = column.type
                    break
            spark_type = self._resolve_spark_type(sqlalchemy_type, prepared_rows, name)
            fields.append(T.StructField(name, spark_type, True))

        schema = T.StructType(fields)
        return data_rows, schema

    def _resolve_spark_type(
        self,
        sqlalchemy_type: sqltypes.TypeEngine | None,
        rows: Sequence[MutableMapping[str, object]],
        column_name: str,
    ):
        import pyspark.sql.types as T  # pragma: no cover - imported lazily

        mapped = self._map_sqlalchemy_to_spark(sqlalchemy_type)
        if mapped is not None:
            return mapped

        sample = None
        for row in rows:
            value = row.get(column_name)
            if value is not None:
                sample = value
                break
        return self._map_python_to_spark(sample)

    def _map_sqlalchemy_to_spark(self, type_: sqltypes.TypeEngine | None):
        import pyspark.sql.types as T  # pragma: no cover - imported lazily

        if type_ is None:
            return None
        if isinstance(type_, sqltypes.Boolean):
            return T.BooleanType()
        if isinstance(type_, (sqltypes.Integer, sqltypes.BigInteger, sqltypes.SmallInteger)):
            return T.LongType()
        if isinstance(type_, sqltypes.Float):
            return T.DoubleType()
        if isinstance(type_, sqltypes.Numeric):
            asdecimal = getattr(type_, "asdecimal", True)
            if asdecimal is False:
                return T.DoubleType()
            precision = getattr(type_, "precision", None) or 38
            scale = getattr(type_, "scale", None) or min(precision, 18)
            precision = min(max(int(precision), 1), 38)
            scale = min(max(int(scale), 0), precision)
            return T.DecimalType(precision=precision, scale=scale)
        if isinstance(type_, sqltypes.DateTime):
            return T.TimestampType()
        if isinstance(type_, sqltypes.Date):
            return T.DateType()
        if isinstance(type_, sqltypes.Time):
            return T.StringType()
        if isinstance(type_, sqltypes.LargeBinary):
            return T.BinaryType()
        length = getattr(type_, "length", None)
        if length and length <= 4000:
            return T.StringType()
        return T.StringType()

    def _map_python_to_spark(self, value: object):
        import pyspark.sql.types as T  # pragma: no cover - imported lazily

        if value is None:
            return T.StringType()
        if isinstance(value, bool):
            return T.BooleanType()
        if isinstance(value, int):
            return T.LongType()
        if isinstance(value, float):
            return T.DoubleType()
        if isinstance(value, Decimal):
            return T.DecimalType(38, 18)
        if isinstance(value, datetime):
            return T.TimestampType()
        if isinstance(value, date):
            return T.DateType()
        return T.StringType()

    def _ensure_schema_exists(self, schema: str | None) -> None:
        if not schema:
            return
        spark = self.spark
        identifier = self._compose_schema_identifier(schema, quote=True)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {identifier}")

    def _compose_schema_identifier(self, schema: str, *, quote: bool) -> str:
        parts = [self._default_catalog, schema] if self._default_catalog else [schema]
        filtered = [part for part in parts if part]
        if quote:
            return ".".join(_quote_identifier(part) for part in filtered)
        return ".".join(filtered)

    def _compose_table_identifier(self, schema: str | None, table: str, *, quote: bool) -> str:
        parts: list[str] = []
        if self._default_catalog:
            parts.append(self._default_catalog)
        if schema:
            parts.append(schema)
        parts.append(table)
        filtered = [part for part in parts if part]
        if quote:
            return ".".join(_quote_identifier(part) for part in filtered)
        return ".".join(filtered)

    def _table_exists(self, schema: str | None, table: str) -> bool:
        spark = self.spark
        identifier = self._compose_table_identifier(schema, table, quote=True)
        try:
            spark.sql(f"DESCRIBE TABLE {identifier}")
        except Exception:
            return False
        return True

    @staticmethod
    def _extract_warehouse_id(http_path: str | None) -> str | None:
        if not http_path:
            return None
        parts = [part for part in http_path.strip("/").split("/") if part]
        if not parts:
            return None
        if "warehouses" in parts:
            index = parts.index("warehouses")
            if index + 1 < len(parts):
                return parts[index + 1]
        return parts[-1] if parts else None


def build_loader_plan(
    *,
    schema: str | None,
    table_name: str,
    replace: bool,
    deduplicate: bool,
) -> LoadPlan:
    return LoadPlan(
        schema or "",
        table_name,
        replace,
        deduplicate,
    )
