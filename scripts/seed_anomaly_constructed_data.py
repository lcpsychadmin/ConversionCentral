from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from app.database import SessionLocal
from app.models import ConstructedField, ConstructedTable
from app.services.constructed_data_store import ConstructedDataStore
from app.services.constructed_data_warehouse import ConstructedDataWarehouse, ConstructedDataWarehouseError


@dataclass(frozen=True)
class ColumnPlan:
    name: str
    data_type: str
    description: str
    is_nullable: bool = True
    anomalies: tuple[str, ...] = ()


@dataclass(frozen=True)
class TablePlan:
    key: str
    name: str
    schema: str
    description: str
    columns: tuple[ColumnPlan, ...]
    row_count: int
    generator: Callable[[int], list[dict[str, Any]]]


def _build_primary_rows(count: int) -> list[dict[str, Any]]:
    today = date.today()
    rows: list[dict[str, Any]] = []
    state_values = ["CA", "NY", "TX", "WA", "OR", "AZ", "NV", "FL"]
    status_variants = [
        "In Progress",
        "IN PROGRESS",
        "in-progress",
        "InProgress",
        "In  Progress",
    ]
    casing_variants = ["ACME CORP", "Acme Corp", "acme corp"]
    standardized_variants = [
        "ACME INC",
        "Acme Inc",
        "ACME,INC",
        "Acme-Inc",
        "acme inc ",
    ]
    for idx in range(count):
        rows.append(
            {
                "record_label": f"ROW-{idx:03d}",
                "suggested_numeric_text": f"{20000 + idx}",
                "non_standard_blank_text": f"filled-{idx}",
                "zip_code_text": f"{90000 + (idx % 100):05d}",
                "zip3_summary": f"{100 + (idx % 50):03d}",
                "multi_pattern_code": f"AA-{idx:04d}",
                "leading_space_flag": f"Value {idx}",
                "quoted_value_flag": f"Alpha {idx}",
                "char_numeric_mix": f"{idx:05d}",
                "char_date_like": (today - timedelta(days=idx % 60)).isoformat(),
                "minor_missing_indicator": f"present-{idx}",
                "minor_divergent_indicator": "STANDARD",
                "boolean_freeform": "TRUE" if idx % 2 == 0 else "FALSE",
                "mostly_unique_code": f"UNIQ-{idx:04d}",
                "standardized_equivalent_name": standardized_variants[idx % len(standardized_variants)],
                "unlikely_event_date": (today - timedelta(days=idx % 90 + 30)).isoformat(),
                "workflow_region_hint": state_values[idx % len(state_values)],
                "contact_reference": f"contact{idx}@sample.org",
                "text_with_numeric_intrusions": "alpha narrative",
                "embedded_list_column": f"task|owner|{idx}",
                "number_with_units_column": f"{50 + (idx % 7)}kg",
                "variant_status_column": status_variants[idx % len(status_variants)],
                "mixed_case_name_column": casing_variants[idx % len(casing_variants)],
                "garbled_name_column": f"Name {idx}",
                "prefixed_name_column": f"Acme {idx}",
                "non_printing_marker": f"marker {idx}",
                "pii_leak_candidate": f"ref-{idx:05d}",
                "never_populated_column": None,
            }
        )

    blank_variants = ["", " ", "MISSING", "??", "(blank)", "[null]", "0000", None]
    for idx, value in enumerate(blank_variants):
        rows[idx]["non_standard_blank_text"] = value

    invalid_zips = ["1234", "ABCDE", "999999", "12-345", "000"]
    for idx, value in enumerate(invalid_zips):
        rows[idx]["zip_code_text"] = value

    invalid_zip3 = ["12", "9A3", "XYZ"]
    for idx, value in enumerate(invalid_zip3):
        rows[idx]["zip3_summary"] = value

    for idx in range(0, count, 25):
        rows[idx]["multi_pattern_code"] = f"{idx:04d}-ZZ"
    for idx in range(10, count, 33):
        rows[idx]["multi_pattern_code"] = f"ZZ{idx:04d}"

    for idx in range(0, min(count, 6)):
        rows[idx]["leading_space_flag"] = "  " + rows[idx]["leading_space_flag"]
        rows[idx]["quoted_value_flag"] = f'"{rows[idx]["quoted_value_flag"]}"'

    for idx, value in enumerate(["", None, "MISSING"]):
        rows[idx]["minor_missing_indicator"] = value

    for idx, value in enumerate(["ALT_A", "ALT_B", "ALT_C"]):
        rows[idx]["minor_divergent_indicator"] = value

    boolean_outliers = ["Y", "N", "??", "1", "0"]
    for idx, value in enumerate(boolean_outliers):
        rows[idx]["boolean_freeform"] = value

    duplicate_pairs = [(0, 60), (1, 61), (2, 62), (3, 63)]
    for left, right in duplicate_pairs:
        if right < count:
            rows[right]["mostly_unique_code"] = rows[left]["mostly_unique_code"]

    rows[0]["unlikely_event_date"] = "1888-01-01"
    rows[1]["unlikely_event_date"] = "2125-05-05"

    rows[0]["workflow_region_hint"] = "CA"
    rows[1]["workflow_region_hint"] = "NY"

    numeric_intrusions = ["123", "4567", "89012"]
    for idx, value in enumerate(numeric_intrusions):
        rows[idx]["text_with_numeric_intrusions"] = value

    for idx in range(0, count, 30):
        rows[idx]["embedded_list_column"] = f"alpha,beta,{idx}"

    for idx in range(0, count, 40):
        rows[idx]["number_with_units_column"] = f"{idx + 10}%"

    garbled_names = ["123456", "987-654", "00000"]
    for idx, value in enumerate(garbled_names):
        rows[idx]["garbled_name_column"] = value

    prefixed = ["#Acme 01", "@Acme", "*Acme"]
    for idx, value in enumerate(prefixed):
        rows[idx]["prefixed_name_column"] = value

    control_chars = ["line\t", "break\n", "bell\x07"]
    for idx, value in enumerate(control_chars):
        rows[idx]["non_printing_marker"] = value

    pii_examples = ["123-45-6789", "555-123-9876", "pii@example.com"]
    for idx, value in enumerate(pii_examples):
        rows[idx]["pii_leak_candidate"] = value

    return rows


def _build_cross_a_rows(count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx in range(count):
        rows.append(
            {
                "record_label": f"CROSSA-{idx:03d}",
                "identifier_major_type": f"MAJ-{idx:03d}",
                "common_measure": round(100.5 + idx, 2),
                "shared_pattern_code": f"AA-{idx:04d}",
            }
        )
    return rows


def _build_cross_b_rows(count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx in range(count):
        rows.append(
            {
                "record_label": f"CROSSB-{idx:03d}",
                "identifier_major_type": 5000 + idx,
                "common_measure": 100 + idx,
                "shared_pattern_code": f"{idx:04d}-ZZ",
            }
        )
    return rows


def _build_recency_rows(count: int, min_age_days: int, max_age_days: int, prefix: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    today = date.today()
    span = max(max_age_days - min_age_days, 1)
    for idx in range(count):
        age = min_age_days + (idx % (span + 1))
        base = today - timedelta(days=age)
        rows.append(
            {
                "record_label": f"{prefix}-{idx:03d}",
                "primary_event_date": base.isoformat(),
                "secondary_event_date": (base - timedelta(days=30 + (idx % 5))).isoformat(),
            }
        )
    return rows


PRIMARY_COLUMNS: tuple[ColumnPlan, ...] = (
    ColumnPlan(
        name="record_label",
        data_type="string",
        description="Synthetic primary key used as the constructed row identifier.",
        is_nullable=False,
    ),
    ColumnPlan(
        name="suggested_numeric_text",
        data_type="string",
        description="Text column whose values are exclusively numeric to trip Suggested_Type and Char_Column_Number_Values.",
        is_nullable=False,
        anomalies=("1001", "1011"),
    ),
    ColumnPlan(
        name="non_standard_blank_text",
        data_type="string",
        description="Values intentionally include empty strings and dummy values to surface Non_Standard_Blanks.",
        anomalies=("1002",),
    ),
    ColumnPlan(
        name="zip_code_text",
        data_type="string",
        description="Mixture of valid and invalid USPS ZIP codes for Invalid_Zip_USA.",
        anomalies=("1003",),
    ),
    ColumnPlan(
        name="zip3_summary",
        data_type="string",
        description="Three digit roll-up values with corrupted patterns for Invalid_Zip3_USA.",
        anomalies=("1024",),
    ),
    ColumnPlan(
        name="multi_pattern_code",
        data_type="string",
        description="Column purposely mixing multiple formats so Column_Pattern_Mismatch can trigger.",
        anomalies=("1007",),
    ),
    ColumnPlan(
        name="leading_space_flag",
        data_type="string",
        description="Values randomly padded with leading spaces for Leading_Spaces.",
        anomalies=("1009",),
    ),
    ColumnPlan(
        name="quoted_value_flag",
        data_type="string",
        description="Values optionally wrapped in quotes to hit Quoted_Values.",
        anomalies=("1010",),
    ),
    ColumnPlan(
        name="char_numeric_mix",
        data_type="string",
        description="Alphabetic column loaded with numeric strings.",
        anomalies=("1011",),
    ),
    ColumnPlan(
        name="char_date_like",
        data_type="string",
        description="Alpha column storing ISO date strings for Char_Column_Date_Values.",
        anomalies=("1012",),
    ),
    ColumnPlan(
        name="minor_missing_indicator",
        data_type="string",
        description="<3% missing/dummy values for Small Missing Value Count.",
        anomalies=("1013",),
    ),
    ColumnPlan(
        name="minor_divergent_indicator",
        data_type="string",
        description="Mostly uniform values with a few outliers to surface Small Divergent Value Count.",
        anomalies=("1014",),
    ),
    ColumnPlan(
        name="boolean_freeform",
        data_type="string",
        description="Boolean intent column including unexpected tokens.",
        anomalies=("1015",),
    ),
    ColumnPlan(
        name="mostly_unique_code",
        data_type="string",
        description="Mostly unique strings with a few duplicates for Potential_Duplicates.",
        anomalies=("1016",),
    ),
    ColumnPlan(
        name="standardized_equivalent_name",
        data_type="string",
        description="Same entity rendered with variant punctuation/casing for Standardized_Value_Matches.",
        anomalies=("1017",),
    ),
    ColumnPlan(
        name="unlikely_event_date",
        data_type="date",
        description="Dates purposely outside 1900..(profile+30yr) for Unlikely_Date_Values.",
        anomalies=("1018",),
    ),
    ColumnPlan(
        name="workflow_region_hint",
        data_type="string",
        description="Column not named state but containing mostly state abbreviations (Unexpected US States).",
        anomalies=("1021",),
    ),
    ColumnPlan(
        name="contact_reference",
        data_type="string",
        description="Column named like a reference yet containing emails (Unexpected Emails).",
        anomalies=("1022", "1100"),
    ),
    ColumnPlan(
        name="text_with_numeric_intrusions",
        data_type="string",
        description="Text column with a few numeric-only values (Small_Numeric_Value_Ct).",
        anomalies=("1023",),
    ),
    ColumnPlan(
        name="embedded_list_column",
        data_type="string",
        description="Delimited payload stored in a single field (Delimited_Data_Embedded).",
        anomalies=("1025",),
    ),
    ColumnPlan(
        name="number_with_units_column",
        data_type="string",
        description="Character column storing numbers plus units/percents (Char_Column_Number_Units).",
        anomalies=("1026",),
    ),
    ColumnPlan(
        name="variant_status_column",
        data_type="string",
        description="Same status expressed in multiple codings (Variant_Coded_Values).",
        anomalies=("1027",),
    ),
    ColumnPlan(
        name="mixed_case_name_column",
        data_type="string",
        description="Entity name toggling casing (Inconsistent_Casing).",
        anomalies=("1028",),
    ),
    ColumnPlan(
        name="garbled_name_column",
        data_type="string",
        description="Name/address column containing purely numeric rows (Non_Alpha_Name_Address).",
        anomalies=("1029",),
    ),
    ColumnPlan(
        name="prefixed_name_column",
        data_type="string",
        description="Entity name starting with punctuation (Non_Alpha_Prefixed_Name).",
        anomalies=("1030",),
    ),
    ColumnPlan(
        name="non_printing_marker",
        data_type="string",
        description="Text column with embedded control characters (Non_Printing_Chars).",
        anomalies=("1031",),
    ),
    ColumnPlan(
        name="pii_leak_candidate",
        data_type="string",
        description="Column seeded with SSNs/phones/emails to trigger Potential_PII.",
        anomalies=("1100",),
    ),
    ColumnPlan(
        name="never_populated_column",
        data_type="string",
        description="Column intentionally left null to satisfy No_Values.",
        anomalies=("1006",),
    ),
)


CROSS_A_COLUMNS = (
    ColumnPlan("record_label", "string", "Synthetic key", False),
    ColumnPlan(
        "identifier_major_type",
        "string",
        "Same column name as cross_b but stored as string here (Multiple_Types_Major).",
        False,
        ("1005",),
    ),
    ColumnPlan(
        "common_measure",
        "decimal",
        "Numeric column stored as DECIMAL so minor type differences emerge across tables.",
        False,
        ("1004",),
    ),
    ColumnPlan(
        "shared_pattern_code",
        "string",
        "Pattern column aligning to AA-#### in this table (Table_Pattern_Mismatch).",
        False,
        ("1008",),
    ),
)

CROSS_B_COLUMNS = (
    ColumnPlan("record_label", "string", "Synthetic key", False),
    ColumnPlan(
        "identifier_major_type",
        "integer",
        "Integer flavor of identifier_major_type.",
        False,
        ("1005",),
    ),
    ColumnPlan(
        "common_measure",
        "integer",
        "Integer flavor of common_measure to surface Multiple_Types_Minor.",
        False,
        ("1004",),
    ),
    ColumnPlan(
        "shared_pattern_code",
        "string",
        "Column with inverted pattern 9999-AA for Table_Pattern_Mismatch.",
        False,
        ("1008",),
    ),
)

RECENCY_COLUMNS = (
    ColumnPlan("record_label", "string", "Synthetic key", False),
    ColumnPlan(
        "primary_event_date",
        "date",
        "First table date column used for recency anomalies.",
        False,
        ("1019", "1020"),
    ),
    ColumnPlan(
        "secondary_event_date",
        "date",
        "Second table date column used for recency anomalies.",
        False,
        ("1019", "1020"),
    ),
)


TABLE_PLANS: tuple[TablePlan, ...] = (
    TablePlan(
        key="primary",
        name="dq_anomaly_profiles_primary",
        schema="dq_anomaly_lab",
        description="Primary synthetic table containing column-level anomaly triggers.",
        columns=PRIMARY_COLUMNS,
        row_count=120,
        generator=_build_primary_rows,
    ),
    TablePlan(
        key="cross_a",
        name="dq_anomaly_profiles_cross_a",
        schema="dq_anomaly_lab",
        description="Half of the cross-table pair for Multiple_Types/Pattern mismatches.",
        columns=CROSS_A_COLUMNS,
        row_count=40,
        generator=_build_cross_a_rows,
    ),
    TablePlan(
        key="cross_b",
        name="dq_anomaly_profiles_cross_b",
        schema="dq_anomaly_lab",
        description="Companion table with conflicting metadata for Multiple_Types/Pattern mismatches.",
        columns=CROSS_B_COLUMNS,
        row_count=40,
        generator=_build_cross_b_rows,
    ),
    TablePlan(
        key="stale",
        name="dq_anomaly_profiles_stale",
        schema="dq_anomaly_lab",
        description="Table whose dates all fall outside the last year to drive Recency_One_Year.",
        columns=RECENCY_COLUMNS,
        row_count=30,
        generator=lambda count: _build_recency_rows(count, 400, 780, "STALE"),
    ),
    TablePlan(
        key="recentish",
        name="dq_anomaly_profiles_recentish",
        schema="dq_anomaly_lab",
        description="Table with dates between six and twelve months ago to trip Recency_Six_Months.",
        columns=RECENCY_COLUMNS,
        row_count=30,
        generator=lambda count: _build_recency_rows(count, 190, 330, "RECENTISH"),
    ),
)


def _write_csv(target_dir: Path, plan: TablePlan, rows: Sequence[dict[str, Any]]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    output_path = target_dir / f"{plan.name}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[column.name for column in plan.columns])
        writer.writeheader()
        for row in rows:
            payload = {}
            for column in plan.columns:
                value = row.get(column.name)
                if value is None:
                    payload[column.name] = ""
                else:
                    payload[column.name] = str(value)
            writer.writerow(payload)


def _ensure_constructed_table(session, plan: TablePlan, reset: bool) -> ConstructedTable:
    table = (
        session.query(ConstructedTable)
        .filter(ConstructedTable.name == plan.name)
        .one_or_none()
    )
    if table is None:
        table = ConstructedTable(
            name=plan.name,
            schema_name=plan.schema,
            description=plan.description,
            status="approved",
        )
        session.add(table)
        session.flush()
    else:
        table.schema_name = plan.schema
        table.description = plan.description
        table.status = "approved"

    desired_signature = tuple(
        (column.name, column.data_type, column.is_nullable)
        for column in plan.columns
    )
    existing_signature = tuple(
        (field.name, field.data_type, field.is_nullable)
        for field in table.fields
    )
    needs_rebuild = reset or existing_signature != desired_signature

    if needs_rebuild:
        for field in list(table.fields):
            session.delete(field)
        session.flush()

    if needs_rebuild:
        for order, column in enumerate(plan.columns):
            session.add(
                ConstructedField(
                    constructed_table_id=table.id,
                    name=column.name,
                    data_type=column.data_type,
                    is_nullable=column.is_nullable,
                    description=column.description,
                    display_order=order,
                )
            )
        session.flush()

    return table


def _seed_store(plans: Iterable[TablePlan], rows_map: dict[str, list[dict[str, Any]]], *, sync_warehouse: bool, reset_metadata: bool) -> None:
    store = ConstructedDataStore()
    warehouse = ConstructedDataWarehouse() if sync_warehouse else None
    for plan in plans:
        with SessionLocal() as session:
            table = _ensure_constructed_table(session, plan, reset_metadata)
            session.commit()
            store.delete_rows_for_table(table.id)
            payloads = [
                {column.name: row.get(column.name) for column in plan.columns}
                for row in rows_map[plan.key]
            ]
            identifiers = [row.get("record_label") for row in rows_map[plan.key]]
            records = store.insert_rows(
                table.id,
                payloads,
                identifiers=identifiers,
            )
            if warehouse:
                try:
                    warehouse.upsert_records(table, records)
                except ConstructedDataWarehouseError as exc:
                    raise RuntimeError(f"Warehouse sync failed for {plan.name}: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic constructed data for anomaly coverage.")
    parser.add_argument("--write-dir", type=Path, help="Directory for CSV exports.")
    parser.add_argument("--seed-store", action="store_true", help="Insert rows via ConstructedDataStore.")
    parser.add_argument("--sync-warehouse", action="store_true", help="After seeding, mirror rows into the constructed warehouse.")
    parser.add_argument("--reset-metadata", action="store_true", help="Drop and recreate ConstructedTable field definitions before seeding.")
    args = parser.parse_args()

    if not args.write_dir and not args.seed_store:
        parser.error("Specify --write-dir, --seed-store, or both.")

    rows_map: dict[str, list[dict[str, Any]]] = {}
    for plan in TABLE_PLANS:
        rows_map[plan.key] = plan.generator(plan.row_count)
        if args.write_dir:
            _write_csv(args.write_dir, plan, rows_map[plan.key])

    if args.seed_store:
        _seed_store(TABLE_PLANS, rows_map, sync_warehouse=args.sync_warehouse, reset_metadata=args.reset_metadata)


if __name__ == "__main__":
    main()
