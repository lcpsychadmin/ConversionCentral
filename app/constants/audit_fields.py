AUDIT_FIELD_DEFINITIONS = (
    {
        "name": "Created By",
        "field_type": "string",
        "field_length": 255,
        "description": "User who created the constructed table definition.",
        "notes": "Auto-generated audit field: Created By",
        "system_required": True,
    },
    {
        "name": "Created Date",
        "field_type": "datetime",
        "description": "UTC timestamp when the constructed table definition was created.",
        "notes": "Auto-generated audit field: Created Date",
        "system_required": True,
    },
    {
        "name": "Modified By",
        "field_type": "string",
        "field_length": 255,
        "description": "User who last modified the constructed table definition.",
        "notes": "Auto-generated audit field: Modified By",
        "system_required": False,
    },
    {
        "name": "Modified Date",
        "field_type": "datetime",
        "description": "UTC timestamp when the constructed table definition was last modified.",
        "notes": "Auto-generated audit field: Modified Date",
        "system_required": False,
    },
)

AUDIT_FIELD_NAME_SET = {spec["name"].lower() for spec in AUDIT_FIELD_DEFINITIONS}
