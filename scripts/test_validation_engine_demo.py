#!/usr/bin/env python
"""Quick demo script that prints validation-rule examples for docs and workshops."""


def test_required_rule():
    """Test required field validation."""
    print("\n=== Testing Required Rule ===")
    
    row_data = {"FirstName": "", "LastName": "Doe"}
    print(f"Testing with data: {row_data}")
    print("Expected: Error on empty FirstName")
    # In real usage, would validate against actual rule


def test_unique_rule():
    """Test unique field validation."""
    print("\n=== Testing Unique Rule ===")
    
    rows = [
        {"Email": "john@example.com"},
        {"Email": "jane@example.com"},
        {"Email": "john@example.com"},  # Duplicate
    ]
    print(f"Testing with {len(rows)} rows")
    print("Row 3 has duplicate email as Row 1")
    print("Expected: Error on Row 3")


def test_range_rule():
    """Test range validation."""
    print("\n=== Testing Range Rule ===")
    
    test_cases = [
        (25, "Valid - within range 0-150"),
        (-5, "Invalid - below minimum"),
        (200, "Invalid - above maximum"),
        ("abc", "Invalid - not a number"),
    ]
    
    for value, description in test_cases:
        print(f"  {description}: {value}")


def test_pattern_rule():
    """Test pattern validation."""
    print("\n=== Testing Pattern Rule ===")
    
    test_cases = [
        ("555-123-4567", "Valid - matches ###-###-#### pattern"),
        ("5551234567", "Invalid - missing hyphens"),
        ("555-12-4567", "Invalid - wrong format"),
        ("abc-def-ghij", "Invalid - contains letters"),
    ]
    
    for value, description in test_cases:
        print(f"  {description}: {value}")


def test_custom_rule():
    """Test custom expression validation."""
    print("\n=== Testing Custom Rule ===")
    
    test_cases = [
        ({"Age": 25, "Status": "Active"}, "Valid - Age > 18 AND Status == 'Active'"),
        ({"Age": 16, "Status": "Active"}, "Invalid - Age not > 18"),
        ({"Age": 25, "Status": "Inactive"}, "Invalid - Status not 'Active'"),
    ]
    
    for row_data, description in test_cases:
        print(f"  {description}: {row_data}")


def test_cross_field_rule():
    """Test cross-field validation."""
    print("\n=== Testing Cross-Field Rule ===")
    
    test_cases = [
        ({"StartDate": "2025-01-01", "EndDate": "2025-12-31"}, "Valid - StartDate <= EndDate"),
        ({"StartDate": "2025-12-31", "EndDate": "2025-01-01"}, "Invalid - EndDate before StartDate"),
    ]
    
    for row_data, description in test_cases:
        print(f"  {description}: {row_data}")


def print_rule_type_examples():
    """Print example configurations for each rule type."""
    print("\n" + "=" * 60)
    print("VALIDATION RULE TYPE EXAMPLES")
    print("=" * 60)
    
    rules = {
        "Required": {
            "description": "Field must not be empty",
            "config": {"fieldName": "FirstName"},
            "examples": ["Required", "Not Null", "Must Fill"],
        },
        "Unique": {
            "description": "Value must be unique across all rows",
            "config": {"fieldName": "Email"},
            "examples": ["Email", "Username", "EmployeeID"],
        },
        "Range": {
            "description": "Numeric value within min/max bounds",
            "config": {"fieldName": "Age", "min": 0, "max": 150},
            "examples": ["Age (0-150)", "Percentage (0-100)", "Price Range"],
        },
        "Pattern": {
            "description": "String matches regex pattern",
            "config": {"fieldName": "Phone", "pattern": "^\\d{3}-\\d{3}-\\d{4}$"},
            "examples": ["Phone (###-###-####)", "Email Format", "ZIP Code"],
        },
        "Custom": {
            "description": "Custom expression evaluates to true",
            "config": {"expression": "Age > 18 AND Status == 'Active'"},
            "examples": ["Complex Logic", "Field Combinations", "Business Rules"],
        },
        "Cross-Field": {
            "description": "Validation comparing multiple fields",
            "config": {"fields": ["StartDate", "EndDate"], "rule": "StartDate <= EndDate"},
            "examples": ["Date Ranges", "Amount Comparisons", "Version Sequencing"],
        },
    }
    
    for rule_type, info in rules.items():
        print(f"\n{rule_type}:")
        print(f"  Description: {info['description']}")
        print(f"  Configuration: {info['config']}")
        print(f"  Use Cases: {', '.join(info['examples'])}")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("VALIDATION ENGINE TEST DEMONSTRATION")
    print("=" * 60)
    
    # Print rule type examples
    print_rule_type_examples()
    
    # Test each rule type
    test_required_rule()
    test_unique_rule()
    test_range_rule()
    test_pattern_rule()
    test_custom_rule()
    test_cross_field_rule()
    
    print("\n" + "=" * 60)
    print("TEST DEMONSTRATION COMPLETE")
    print("=" * 60)
    print("\nTo run actual validation tests against the database:")
    print("1. Create validation rules via API: POST /constructed-data-validation-rules")
    print("2. Call batch save endpoint: POST /constructed-data/{table_id}/batch-save")
    print("3. Check response for validation errors")
    print("\nFor API documentation, see: VALIDATION_ENGINE.md")


if __name__ == "__main__":
    main()
