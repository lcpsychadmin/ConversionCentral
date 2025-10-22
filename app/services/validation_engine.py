"""
Validation engine for constructed table data.

Evaluates validation rules against row data and returns validation errors.
Supports 6 rule types:
- required: Field must not be empty
- unique: Value must be unique across all rows
- range: Numeric value between min and max
- pattern: String matches regex pattern
- custom: Custom expression/formula evaluation
- cross_field: Validation across multiple fields
"""

import re
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from app.models import ConstructedData, ConstructedDataValidationRule, ConstructedTable


class ValidationError:
    """Represents a validation error for a row/field."""

    def __init__(self, row_index: int, field_name: str | None, message: str, rule_id: UUID):
        self.row_index = row_index
        self.field_name = field_name
        self.message = message
        self.rule_id = rule_id

    def dict(self) -> dict[str, Any]:
        return {
            "rowIndex": self.row_index,
            "fieldName": self.field_name,
            "message": self.message,
            "ruleId": str(self.rule_id),
        }


class ValidationEngine:
    """Engine for evaluating validation rules against constructed data."""

    def __init__(self, db: Session):
        self.db = db

    def validate_row(
        self,
        row_index: int,
        row_data: dict[str, Any],
        constructed_table_id: UUID,
        is_new_row: bool = True,
    ) -> list[ValidationError]:
        """
        Validate a single row against all active rules for the table.

        Args:
            row_index: Index of the row (for error reporting)
            row_data: Dictionary of field_name -> value pairs
            constructed_table_id: ID of the constructed table
            is_new_row: Whether this is a new row (for applies_to_new_only filtering)

        Returns:
            List of ValidationError objects (empty if all validations pass)
        """
        errors: list[ValidationError] = []

        # Get all active rules for this table
        rules = (
            self.db.query(ConstructedDataValidationRule)
            .filter(
                ConstructedDataValidationRule.constructed_table_id == constructed_table_id,
                ConstructedDataValidationRule.is_active == True,  # noqa: E712
            )
            .all()
        )

        for rule in rules:
            # Skip applies_to_new_only rules if updating existing row
            if rule.applies_to_new_only and not is_new_row:
                continue

            # Evaluate the rule
            rule_errors = self._evaluate_rule(row_index, row_data, rule)
            errors.extend(rule_errors)

        return errors

    def validate_batch(
        self,
        rows: list[dict[str, Any]],
        constructed_table_id: UUID,
        is_new_rows: bool = True,
    ) -> list[ValidationError]:
        """
        Validate multiple rows against all active rules for the table.

        Args:
            rows: List of row data dictionaries
            constructed_table_id: ID of the constructed table
            is_new_rows: Whether these are new rows

        Returns:
            List of ValidationError objects (empty if all validations pass)
        """
        all_errors: list[ValidationError] = []

        for row_index, row_data in enumerate(rows):
            row_errors = self.validate_row(row_index, row_data, constructed_table_id, is_new_rows)
            all_errors.extend(row_errors)

        return all_errors

    def _evaluate_rule(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """Dispatch to appropriate rule type validator."""
        if rule.rule_type == "required":
            return self._validate_required(row_index, row_data, rule)
        elif rule.rule_type == "unique":
            return self._validate_unique(row_index, row_data, rule)
        elif rule.rule_type == "range":
            return self._validate_range(row_index, row_data, rule)
        elif rule.rule_type == "pattern":
            return self._validate_pattern(row_index, row_data, rule)
        elif rule.rule_type == "custom":
            return self._validate_custom(row_index, row_data, rule)
        elif rule.rule_type == "cross_field":
            return self._validate_cross_field(row_index, row_data, rule)
        else:
            return [
                ValidationError(
                    row_index, None, f"Unknown rule type: {rule.rule_type}", rule.id
                )
            ]

    def _validate_required(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """
        Validate required field rule.

        Config: { "fieldName": "FirstName" }
        """
        config = rule.configuration
        field_name = config.get("fieldName")

        if not field_name:
            return [
                ValidationError(
                    row_index, None, "Required rule missing fieldName in configuration", rule.id
                )
            ]

        value = row_data.get(field_name)

        # Check if value is empty/null
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return [
                ValidationError(row_index, field_name, rule.error_message, rule.id)
            ]

        return []

    def _validate_unique(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """
        Validate unique field rule.

        Config: { "fieldName": "Email" }
        """
        config = rule.configuration
        field_name = config.get("fieldName")

        if not field_name:
            return [
                ValidationError(
                    row_index, None, "Unique rule missing fieldName in configuration", rule.id
                )
            ]

        value = row_data.get(field_name)

        # Skip validation if value is null (null can appear multiple times)
        if value is None:
            return []

        # Count rows with the same value for this field in this table
        # Query payload field for the given field_name and count matches
        count = (
            self.db.query(ConstructedData)
            .filter(
                ConstructedData.constructed_table_id == rule.constructed_table_id,
            )
            .all()
        )

        # Count how many rows have this value
        matching_count = sum(
            1 for row in count
            if row.payload.get(field_name) == value
        )

        if matching_count > 1:
            return [
                ValidationError(
                    row_index, field_name, rule.error_message, rule.id
                )
            ]

        return []

    def _validate_range(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """
        Validate range rule for numeric fields.

        Config: { "fieldName": "Age", "min": 0, "max": 150 }
        """
        config = rule.configuration
        field_name = config.get("fieldName")
        min_val = config.get("min")
        max_val = config.get("max")

        if not field_name:
            return [
                ValidationError(
                    row_index, None, "Range rule missing fieldName in configuration", rule.id
                )
            ]

        value = row_data.get(field_name)

        # Skip validation if value is null
        if value is None:
            return []

        # Try to convert to number
        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return [
                ValidationError(
                    row_index, field_name, f"Value '{value}' is not a valid number", rule.id
                )
            ]

        # Check min
        if min_val is not None:
            try:
                min_num = float(min_val)
                if num_value < min_num:
                    return [
                        ValidationError(
                            row_index, field_name, rule.error_message, rule.id
                        )
                    ]
            except (ValueError, TypeError):
                pass

        # Check max
        if max_val is not None:
            try:
                max_num = float(max_val)
                if num_value > max_num:
                    return [
                        ValidationError(
                            row_index, field_name, rule.error_message, rule.id
                        )
                    ]
            except (ValueError, TypeError):
                pass

        return []

    def _validate_pattern(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """
        Validate regex pattern rule for string fields.

        Config: { "fieldName": "Phone", "pattern": "^\\d{3}-\\d{3}-\\d{4}$" }
        """
        config = rule.configuration
        field_name = config.get("fieldName")
        pattern = config.get("pattern")

        if not field_name:
            return [
                ValidationError(
                    row_index, None, "Pattern rule missing fieldName in configuration", rule.id
                )
            ]

        if not pattern:
            return [
                ValidationError(
                    row_index, None, "Pattern rule missing pattern in configuration", rule.id
                )
            ]

        value = row_data.get(field_name)

        # Skip validation if value is null
        if value is None:
            return []

        # Convert to string if needed
        str_value = str(value)

        # Check if pattern matches
        try:
            if not re.match(pattern, str_value):
                return [
                    ValidationError(
                        row_index, field_name, rule.error_message, rule.id
                    )
                ]
        except re.error as e:
            return [
                ValidationError(
                    row_index, None, f"Invalid regex pattern: {str(e)}", rule.id
                )
            ]

        return []

    def _validate_custom(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """
        Validate custom expression rule.

        Config: { "expression": "field1 > 100 AND field2 < 50" }

        Supports simple expressions with field names and basic operators.
        WARNING: This uses eval() which has security implications.
        Only use with trusted rule configurations from your database.
        """
        config = rule.configuration
        expression = config.get("expression")

        if not expression:
            return [
                ValidationError(
                    row_index, None, "Custom rule missing expression in configuration", rule.id
                )
            ]

        # Create a safe namespace for eval with row data
        # Only allow field access and basic operators
        safe_dict = {
            "field": row_data,
            **row_data,  # Allow direct field access by name
        }

        try:
            # Evaluate the expression
            result = eval(expression, {"__builtins__": {}}, safe_dict)

            if not result:
                return [
                    ValidationError(
                        row_index, None, rule.error_message, rule.id
                    )
                ]
        except Exception as e:
            return [
                ValidationError(
                    row_index, None, f"Error evaluating custom rule: {str(e)}", rule.id
                )
            ]

        return []

    def _validate_cross_field(
        self, row_index: int, row_data: dict[str, Any], rule: ConstructedDataValidationRule
    ) -> list[ValidationError]:
        """
        Validate cross-field rule (validation across multiple fields).

        Config: { "fields": ["StartDate", "EndDate"], "rule": "StartDate <= EndDate" }

        Supports simple comparison rules between fields.
        """
        config = rule.configuration
        fields = config.get("fields", [])
        validation_rule = config.get("rule")

        if not fields or not validation_rule:
            return [
                ValidationError(
                    row_index, None, "Cross-field rule missing fields or rule in configuration", rule.id
                )
            ]

        # Create namespace with field values
        safe_dict = {
            field: row_data.get(field) for field in fields
        }

        try:
            result = eval(validation_rule, {"__builtins__": {}}, safe_dict)

            if not result:
                return [
                    ValidationError(
                        row_index, None, rule.error_message, rule.id
                    )
                ]
        except Exception as e:
            return [
                ValidationError(
                    row_index, None, f"Error evaluating cross-field rule: {str(e)}", rule.id
                )
            ]

        return []
