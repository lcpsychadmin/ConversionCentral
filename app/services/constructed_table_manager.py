"""Service for managing constructed tables in target SQL Server databases."""
from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.schemas import SystemConnectionType
from app.services.connection_resolver import (
    UnsupportedConnectionError,
    resolve_sqlalchemy_url,
)

if TYPE_CHECKING:
    from app.models import ConstructedField, ConstructedTable

logger = getLogger(__name__)


class ConstructedTableManagerError(Exception):
    """Raised when managing a constructed table fails."""


def _map_field_type_to_sql_server(data_type: str, is_nullable: bool = True) -> str:
    """
    Map Python/application data types to SQL Server types.
    
    Args:
        data_type: The application data type (e.g., 'string', 'integer', 'datetime', 'decimal')
        is_nullable: Whether the field allows NULL values
        
    Returns:
        SQL Server data type definition
    """
    type_mapping = {
        "string": "NVARCHAR(MAX)",
        "varchar": "NVARCHAR(MAX)",
        "text": "NVARCHAR(MAX)",
        "integer": "INT",
        "int": "INT",
        "bigint": "BIGINT",
        "decimal": "DECIMAL(18, 4)",
        "float": "FLOAT",
        "double": "FLOAT",
        "boolean": "BIT",
        "bool": "BIT",
        "date": "DATE",
        "datetime": "DATETIME2",
        "timestamp": "DATETIME2",
        "time": "TIME",
        "json": "NVARCHAR(MAX)",
        "uuid": "UNIQUEIDENTIFIER",
    }

    sql_type = type_mapping.get(data_type.lower(), "NVARCHAR(MAX)")
    return sql_type


def _generate_create_table_sql(
    schema_name: str, table_name: str, fields: list[ConstructedField]
) -> str:
    """
    Generate a CREATE TABLE statement for a constructed table.
    
    Args:
        schema_name: The schema name (e.g., 'dbo')
        table_name: The table name
        fields: List of ConstructedField objects defining the table structure
        
    Returns:
        SQL CREATE TABLE statement
    """
    if not fields:
        raise ConstructedTableManagerError("Cannot create table without fields")

    column_definitions = []
    for field in fields:
        sql_type = _map_field_type_to_sql_server(field.data_type, field.is_nullable)
        nullable = "NULL" if field.is_nullable else "NOT NULL"
        
        default_clause = ""
        if field.default_value:
            default_clause = f" DEFAULT '{field.default_value}'"
        
        col_def = f"[{field.name}] {sql_type} {nullable}{default_clause}"
        column_definitions.append(col_def)

    columns_sql = ",\n    ".join(column_definitions)
    
    # Always add audit columns
    create_sql = f"""CREATE TABLE [{schema_name}].[{table_name}] (
    {columns_sql},
    [created_at] DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    [updated_at] DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT [PK_{table_name}] PRIMARY KEY (
        [created_at]
    )
)"""
    
    return create_sql


def _generate_drop_table_sql(schema_name: str, table_name: str) -> str:
    """Generate a DROP TABLE IF EXISTS statement."""
    return f"IF OBJECT_ID('[{schema_name}].[{table_name}]', 'U') IS NOT NULL DROP TABLE [{schema_name}].[{table_name}]"


def _generate_alter_table_sql(
    schema_name: str, table_name: str, current_fields: list[ConstructedField], new_fields: list[ConstructedField]
) -> list[str]:
    """
    Generate ALTER TABLE statements to modify a table's structure.
    
    This is a simplified version that drops and recreates the table.
    In production, you'd want more sophisticated schema migration logic.
    """
    # For now, we'll drop and recreate
    # In future, implement smarter column addition/removal/modification
    statements = [
        _generate_drop_table_sql(schema_name, table_name),
        _generate_create_table_sql(schema_name, table_name, new_fields)
    ]
    return statements


def create_or_update_constructed_table(
    connection_type: SystemConnectionType,
    connection_string: str,
    schema_name: str,
    table_name: str,
    fields: list[ConstructedField],
) -> None:
    """
    Create or update a constructed table in the target database.
    
    If the table exists, it will be dropped and recreated with the new schema.
    
    Args:
        connection_type: The type of connection (e.g., JDBC)
        connection_string: The connection string to the target database
        schema_name: The schema name (e.g., 'dbo')
        table_name: The table name
        fields: List of ConstructedField objects defining the table structure
        
    Raises:
        ConstructedTableManagerError: If the operation fails
    """
    if not fields:
        raise ConstructedTableManagerError("Cannot create table without fields")

    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise ConstructedTableManagerError(str(exc)) from exc

    engine = create_engine(url, pool_pre_ping=True)

    try:
        with engine.connect() as connection:
            # Drop existing table if it exists
            drop_sql = _generate_drop_table_sql(schema_name, table_name)
            logger.info(f"Executing: {drop_sql}")
            connection.execute(text(drop_sql))
            
            # Create the table
            create_sql = _generate_create_table_sql(schema_name, table_name, fields)
            logger.info(f"Executing: {create_sql}")
            connection.execute(text(create_sql))
            
            connection.commit()
            logger.info(f"Successfully created table [{schema_name}].[{table_name}]")
    except SQLAlchemyError as exc:
        logger.error(f"Failed to create/update table: {exc}")
        raise ConstructedTableManagerError(f"Failed to create/update table: {exc}") from exc
    finally:
        engine.dispose()


def drop_constructed_table(
    connection_type: SystemConnectionType,
    connection_string: str,
    schema_name: str,
    table_name: str,
) -> None:
    """
    Drop a constructed table from the target database.
    
    Args:
        connection_type: The type of connection (e.g., JDBC)
        connection_string: The connection string to the target database
        schema_name: The schema name (e.g., 'dbo')
        table_name: The table name
        
    Raises:
        ConstructedTableManagerError: If the operation fails
    """
    try:
        url = resolve_sqlalchemy_url(connection_type, connection_string)
    except UnsupportedConnectionError as exc:
        raise ConstructedTableManagerError(str(exc)) from exc

    engine = create_engine(url, pool_pre_ping=True)

    try:
        with engine.connect() as connection:
            drop_sql = _generate_drop_table_sql(schema_name, table_name)
            logger.info(f"Executing: {drop_sql}")
            connection.execute(text(drop_sql))
            connection.commit()
            logger.info(f"Successfully dropped table [{schema_name}].[{table_name}]")
    except SQLAlchemyError as exc:
        logger.error(f"Failed to drop table: {exc}")
        raise ConstructedTableManagerError(f"Failed to drop table: {exc}") from exc
    finally:
        engine.dispose()
