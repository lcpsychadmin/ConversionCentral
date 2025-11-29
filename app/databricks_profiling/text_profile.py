"""Helpers for computing rich text profiling metrics inside Databricks notebooks."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

try:  # pragma: no cover - pyspark is only available inside Databricks
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover - allows local tooling without pyspark
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover - typing helper only
    from pyspark.sql import DataFrame as SparkDataFrame
else:  # pragma: no cover - fallback when pyspark not installed
    SparkDataFrame = Any


def _require_pyspark() -> None:
    if F is None or T is None:  # pragma: no cover - guarded by Databricks runtime
        raise RuntimeError("pyspark must be installed to build text profiling metrics.")


def _safe_ratio(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator in (None, "") or denominator in (None, "", 0):
        return None
    try:
        value = float(numerator) / float(denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return value if value >= 0 else None


def _shape_pattern(value: str | None) -> str | None:
    if value is None:
        return None
    result: list[str] = []
    for char in value:
        if char.isalpha():
            result.append("A" if char.isupper() else "a")
        elif char.isdigit():
            result.append("9")
        elif char.isspace():
            result.append(" ")
        elif char in {"-", "_"}:
            result.append(char)
        else:
            result.append("#")
    return "".join(result) if result else None


_DUMMY_LITERAL_VALUES = (
    "blank",
    "error",
    "missing",
    "tbd",
    "n/a",
    "na",
    "#na",
    "none",
    "null",
    "unknown",
    "(blank)",
    "(error)",
    "(missing)",
    "(tbd)",
    "(n/a)",
    "(#na)",
    "(none)",
    "(null)",
    "(unknown)",
    "[blank]",
    "[error]",
    "[missing]",
    "[tbd]",
    "[n/a]",
    "[#na]",
    "[none]",
    "[null]",
    "[unknown]",
    ".",
    "?",
)

_PATTERN_UDF = None
if F is not None and T is not None:  # pragma: no cover - executed inside Databricks only
    _PATTERN_UDF = F.udf(_shape_pattern, T.StringType())


def _as_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _stat_entry(label: str, count: int, total: int | None) -> dict[str, Any]:
    return {
        "label": label,
        "count": count or 0,
        "percentage": _safe_ratio(count, total),
    }


def build_text_profile(
    df: SparkDataFrame,
    column: str,
    *,
    row_count: int,
    null_count: int,
    non_null_count: int,
    min_length: int | None,
    max_length: int | None,
    avg_length: float | None,
    min_text: str | None,
    max_text: str | None,
    pattern_limit: int = 6,
    length_histogram_limit: int = 25,
) -> dict[str, Any] | None:
    """Compute TestGen-style text profiling metrics for the provided column."""

    _require_pyspark()
    assert F is not None  # hint for type-checkers

    total_rows = max(int(row_count or 0), 0)
    value_count = max(int(non_null_count or 0), 0)
    total_nulls = max(int(null_count or 0), 0)
    if total_rows <= 0:
        return None

    value_df = df.select(F.col(column).alias("value"))
    value_col = F.col("value")
    non_null_df = value_df.where(value_col.isNotNull())
    trimmed = F.trim(value_col)
    lower_trimmed = F.lower(trimmed)
    letters_only = F.regexp_replace(value_col, "[^A-Za-z]", "")
    digits_only = F.regexp_replace(trimmed, "[^0-9]", "")
    space_count = F.length(value_col) - F.length(F.regexp_replace(value_col, "\\s", ""))
    dummy_literal_condition = lower_trimmed.isin(*_DUMMY_LITERAL_VALUES)
    dummy_pattern_condition = trimmed.rlike(r"^([\-.0x9z])\\1{1,}$")
    dummy_condition = dummy_literal_condition | dummy_pattern_condition
    includes_digits_condition = trimmed.rlike(r".*\\d.*")
    parsed_timestamp = F.coalesce(
        F.to_timestamp(trimmed),
        F.to_timestamp(trimmed, "yyyy-MM-dd"),
        F.to_timestamp(trimmed, "MM/dd/yyyy"),
        F.to_timestamp(trimmed, "yyyy/MM/dd"),
    )
    blank_count = whitespace_count = numeric_only_count = 0
    zero_count = quoted_count = leading_space_count = 0
    embedded_space_count = upper_case_count = lower_case_count = 0
    non_alpha_count = dummy_value_count = includes_digit_count = 0
    date_value_count = 0
    space_total = 0.0
    if value_count > 0:
        aggregates = non_null_df.agg(
            F.sum(F.when(F.length(value_col) == 0, 1).otherwise(0)).alias("blank_count"),
            F.sum(
                F.when((F.length(trimmed) == 0) & (F.length(value_col) > 0), 1).otherwise(0)
            ).alias("whitespace_count"),
            F.sum(F.when((F.length(trimmed) > 0) & (digits_only == trimmed), 1).otherwise(0)).alias(
                "numeric_only_count"
            ),
            F.sum(F.when(F.upper(trimmed) == "0", 1).otherwise(0)).alias("zero_count"),
            F.sum(F.when(trimmed.rlike(r"^(['\"]).*\\1$"), 1).otherwise(0)).alias("quoted_count"),
            F.sum(F.when(value_col.rlike(r"^\\s+"), 1).otherwise(0)).alias("leading_space_count"),
            F.sum(F.when(space_count > 0, 1).otherwise(0)).alias("embedded_space_count"),
            F.sum(space_count).alias("space_total"),
            F.sum(F.when(dummy_condition, 1).otherwise(0)).alias("dummy_value_count"),
            F.sum(F.when(includes_digits_condition, 1).otherwise(0)).alias("includes_digit_count"),
            F.sum(F.when(parsed_timestamp.isNotNull(), 1).otherwise(0)).alias("date_value_count"),
            F.sum(
                F.when(
                    (F.length(letters_only) > 0)
                    & (letters_only == F.upper(letters_only))
                    & (letters_only != F.lower(letters_only)),
                    1,
                ).otherwise(0)
            ).alias("upper_case_count"),
            F.sum(
                F.when(
                    (F.length(letters_only) > 0)
                    & (letters_only == F.lower(letters_only))
                    & (letters_only != F.upper(letters_only)),
                    1,
                ).otherwise(0)
            ).alias("lower_case_count"),
            F.sum(F.when(F.length(letters_only) == 0, 1).otherwise(0)).alias("non_alpha_count"),
        ).collect()[0]

        blank_count = _as_int(aggregates["blank_count"])
        whitespace_count = _as_int(aggregates["whitespace_count"])
        numeric_only_count = _as_int(aggregates["numeric_only_count"])
        zero_count = _as_int(aggregates["zero_count"])
        quoted_count = _as_int(aggregates["quoted_count"])
        leading_space_count = _as_int(aggregates["leading_space_count"])
        embedded_space_count = _as_int(aggregates["embedded_space_count"])
        space_total = _as_float(aggregates["space_total"])
        dummy_value_count = _as_int(aggregates["dummy_value_count"])
        includes_digit_count = _as_int(aggregates["includes_digit_count"])
        date_value_count = _as_int(aggregates["date_value_count"])
        upper_case_count = _as_int(aggregates["upper_case_count"])
        lower_case_count = _as_int(aggregates["lower_case_count"])
        non_alpha_count = _as_int(aggregates["non_alpha_count"])

    zero_length_count = blank_count + whitespace_count
    missing_count = total_nulls + zero_length_count + dummy_value_count
    duplicate_rows = 0
    if value_count > 0:
        duplicates_row = (
            value_df.where(value_col.isNotNull())
            .groupBy("value")
            .agg(F.count("*").alias("count"))
            .where(F.col("count") > 1)
            .agg(F.sum(F.col("count") - 1).alias("duplicate_rows"))
            .collect()[0]
        )
        duplicate_rows = _as_int(duplicates_row["duplicate_rows"]) if duplicates_row["duplicate_rows"] is not None else 0

    unique_rows = max(value_count - duplicate_rows, 0)
    mixed_case_count = max(value_count - upper_case_count - lower_case_count - non_alpha_count, 0)

    pattern_rows: list[Any] = []
    distinct_patterns = 0
    standard_pattern_matches = 0
    if value_count > 0 and _PATTERN_UDF is not None:
        pattern_df = value_df.select(_PATTERN_UDF(trimmed).alias("pattern")).where(F.col("pattern").isNotNull())
        distinct_patterns = pattern_df.select("pattern").distinct().count()
        pattern_rows = (
            pattern_df.groupBy("pattern")
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc())
            .limit(pattern_limit)
            .collect()
        )
        standard_pattern_matches = _as_int(pattern_rows[0]["count"]) if pattern_rows else 0

    frequent_patterns = [
        {
            "label": row["pattern"] or "(empty)",
            "count": _as_int(row["count"]),
            "percentage": _safe_ratio(row["count"], value_count),
        }
        for row in pattern_rows
    ]

    length_histogram = []
    if value_count > 0:
        length_rows = (
            value_df.where(value_col.isNotNull())
            .select(F.length(value_col).alias("length"))
            .groupBy("length")
            .agg(F.count("*").alias("count"))
            .orderBy("length")
            .limit(length_histogram_limit)
            .collect()
        )
        for row in length_rows:
            length_histogram.append(
                {
                    "label": str(int(row["length"])),
                    "count": _as_int(row["count"]),
                    "lower": int(row["length"]),
                    "upper": int(row["length"]),
                }
            )

    actual_value_count = max(value_count - zero_length_count - dummy_value_count, 0)

    stats = {
        "record_count": total_rows,
        "value_count": value_count,
        "actual_value_count": actual_value_count,
        "null_value_count": total_nulls,
        "zero_length_count": zero_length_count,
        "dummy_value_count": dummy_value_count,
        "missing_count": missing_count,
        "missing_percentage": _safe_ratio(missing_count, total_rows),
        "duplicate_count": duplicate_rows,
        "duplicate_percentage": _safe_ratio(duplicate_rows, value_count),
        "zero_count": zero_count,
        "numeric_only_count": numeric_only_count,
        "includes_digit_count": includes_digit_count,
        "quoted_count": quoted_count,
        "leading_space_count": leading_space_count,
        "embedded_space_count": embedded_space_count,
        "date_value_count": date_value_count,
        "average_embedded_spaces": _safe_ratio(space_total, value_count),
        "min_length": min_length,
        "max_length": max_length,
        "avg_length": avg_length,
        "min_text": min_text,
        "max_text": max_text,
        "distinct_patterns": distinct_patterns or None,
        "standard_pattern_matches": standard_pattern_matches or None,
    }

    missing_breakdown = []
    if total_nulls:
        missing_breakdown.append(_stat_entry("Null", total_nulls, total_rows))
    if zero_length_count:
        missing_breakdown.append(_stat_entry("Zero length", zero_length_count, total_rows))
    if dummy_value_count:
        missing_breakdown.append(_stat_entry("Dummy values", dummy_value_count, total_rows))

    duplicate_breakdown = []
    if value_count:
        duplicate_breakdown.append(_stat_entry("Duplicate values", duplicate_rows, value_count))
        duplicate_breakdown.append(_stat_entry("Unique values", unique_rows, value_count))

    case_breakdown = []
    if upper_case_count:
        case_breakdown.append(_stat_entry("Upper Case", upper_case_count, value_count))
    if lower_case_count:
        case_breakdown.append(_stat_entry("Lower Case", lower_case_count, value_count))
    if mixed_case_count:
        case_breakdown.append(_stat_entry("Mixed Case", mixed_case_count, value_count))
    if non_alpha_count:
        case_breakdown.append(_stat_entry("Non Alpha", non_alpha_count, value_count))

    text_profile = {
        "stats": stats,
        "missing_breakdown": missing_breakdown,
        "duplicate_breakdown": duplicate_breakdown,
        "case_breakdown": case_breakdown,
        "frequent_patterns": frequent_patterns,
        "length_histogram": length_histogram,
    }
    return text_profile


__all__ = ["build_text_profile"]
