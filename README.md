# Conversion Central API

Conversion Central is a FastAPI-powered backend that provides a reusable framework for orchestrating enterprise data migration projects. It models the execution hierarchy (Project → Release → Mock Cycle → Execution Context) alongside the data definition hierarchy (Process Area → Data Object) and exposes fully CRUD-capable REST endpoints for each entity.

## Features

- FastAPI application with modular routers for all core entities
- SQLAlchemy ORM models backed by PostgreSQL
- Alembic migrations for schema management
- Pydantic schemas for request/response validation
- Cascading foreign keys to maintain referential integrity
- Pytest suite that exercises CRUD flows end-to-end with an in-memory database
- Metadata modeling for source/target systems, tables, and fields
- Release-specific field inclusion controls via Field Load management
- Validation layer capturing pre- and post-load results with detailed issue tracking
- Ingestion layer for managing system connections and orchestrating ingestion jobs
- Governance approvals tracking for validation sign-off workflows
- Constructed data workspace for curated supplemental tables, fields, approvals, and row-level payloads
- Table load sequencing with contiguous order enforcement, optional approvals, and DAG-aware scheduling
- Security layer with role-based access control at the process area scope

## Project Structure

```
app/
  config.py          # Application settings (DATABASE_URL, etc.)
  database.py        # SQLAlchemy engine/session configuration
  main.py            # FastAPI app entrypoint
  models/            # SQLAlchemy ORM models
  schemas/           # Pydantic schemas
  routers/           # FastAPI routers per entity
migrations/          # Alembic environment and migration scripts
requirements.txt     # Python dependencies
tests/               # Pytest-based API tests
```

## Getting Started

For production guidance on Azure Container Apps, see `docs/azure-container-apps.md`. It walks through building images, configuring managed databases, and deploying backend/frontend workloads.

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 13+

Create a PostgreSQL database (default name: `conversion_central`) and ensure it is reachable with a user that can create schemas.

### 2. Install Dependencies

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Configure Environment

Copy the sample below into an `.env` file at the project root or export equivalent environment variables:

```
DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/conversion_central
APP_NAME=Conversion Central API
# Optional: override default CORS allow list (comma separated)
FRONTEND_ORIGINS=http://localhost:5173,http://localhost:3000,https://wescollins.duckdns.org
```

Adjust credentials, host, port, and database name to match your environment.

### 4. Run Database Migrations

```powershell
alembic upgrade head
```

This applies the latest schema changes, including the constructed data domain, table load sequencing with approvals, and the security layer (roles, users, and process area role assignments) alongside the validation and ingestion layers.

### 5. Launch the API

```powershell
uvicorn app.main:app --reload
```

Visit `http://127.0.0.1:8000/docs` for interactive API documentation.

### Metadata Endpoints

The platform now tracks upstream and downstream systems alongside physical data structures. After starting the server, you can exercise the following routers directly from the Swagger UI or via HTTP clients:

- `POST /systems` – register a source/target system with governance metadata.
- `POST /tables` – describe a physical table (schema, classification) within a system.
- `POST /fields` – capture column-level details such as data type, length, and validation.
- `POST /data-object-systems` – bind curated data objects to their source or target systems.
- `POST /field-loads` – toggle field participation for individual releases.

Each endpoint also supports `GET`, `PUT`, and `DELETE` operations for full lifecycle management.

### Sequencing Endpoints

Control the intra-object execution order and approval workflow for table loads:

- `POST /table-load-orders` – assign tables to contiguous sequence slots per data object; the API automatically normalizes positions on create, update, and delete.
- `POST /table-load-order-approvals` – record SME, Data Owner, Approver, or Admin decisions for a given table sequence proposal.
- `GET /dependency-dag/tables` – retrieve the DAG-based table execution plan, which now honors declared table load orders before falling back to alphabetical ordering.

### Validation Endpoints

Capture pre- and post-load assessment outcomes with the following routers:

- `POST /pre-load-validation-results` – register summary metrics for a pre-load validation run tied to a release.
- `POST /pre-load-validation-issues` – record granular pre-load issues, including severity and resolution notes.
- `POST /pre-load-validation-approvals` – capture sign-off decisions for each pre-load validation result.
- `POST /post-load-validation-results` – store post-load validation metrics once target populations are loaded.
- `POST /post-load-validation-issues` – track post-load discrepancies for follow-up and reconciliation.
- `POST /post-load-validation-approvals` – document downstream approvals required after load verification.

All validation routes expose the full CRUD surface so you can iterate on findings, approvals, and resolutions over time. Execution contexts cannot be promoted to `ReadyForLoad` until every pre-load validation approval for the associated release is in the `approved` state, ensuring governance sign-off precedes migration activity.

### Constructed Data Endpoints

Business users can now curate supplemental data assets that feed migration execution. All constructed tables must receive at least one approval before data rows can be created or updated, ensuring governance over enrichment datasets.

- `POST /constructed-tables` – establish a curated table tied to an execution context, with lifecycle status management.
- `POST /constructed-fields` – define the schema for each constructed table, including data types and defaults.
- `POST /constructed-table-approvals` – capture sign-off decisions from data stewards, business owners, or technical leads.
- `POST /constructed-data` – load row-level payloads (JSON) for approved tables only.

Each endpoint supports the complete CRUD surface and enforces foreign key integrity across execution contexts, approvals, and users.

### Security Endpoints

Role-based access can now be enforced per process area to control who may view or edit migration assets.

- `POST /roles` – manage reusable RBAC roles such as Process Owner or Data Custodian.
- `POST /users` – register users who participate in governance workflows (name, email, status).
- `POST /process-area-role-assignments` – bind a user and role to a process area, optionally capturing who granted access.

All security routes support list, update, and delete operations while enforcing uniqueness and referential integrity across process areas, users, and roles.

### Ingestion Endpoints

Configure connectivity and monitor ingestion progress:

- `POST /system-connections` – define how to authenticate and connect to a registered system (JDBC, ODBC, API, file, SAP RFC, etc.).
- `POST /ingestion-jobs` – schedule or log table-level ingestion executions with status tracking and row-count metrics.

Both endpoints support list, update, and delete verbs, enabling full lifecycle management of connectivity and job telemetry.

### 6. Run Tests

The test suite runs against an in-memory SQLite database and covers CRUD flows for every endpoint.

```powershell
pytest
```

The suite exercises every CRUD router, including metadata, mapping, field load, validation, approval, ingestion, and sequencing endpoints to ensure regression safety and covers DAG ordering logic.

## Entity Overview

| Entity             | Description                                      | Key Relationships                           |
|--------------------|--------------------------------------------------|---------------------------------------------|
| Project            | Top-level migration initiative                   | Has many Releases and Process Areas         |
| Release            | Delivery tranche within a Project                | Belongs to Project; has many Mock Cycles    |
| Mock Cycle         | Dress rehearsal iteration for a Release          | Belongs to Release; has many Execution Contexts |
| Execution Context  | Specific execution instance of a Mock Cycle      | Belongs to Mock Cycle                       |
| Role               | Named RBAC role that governs access               | Has many Process Area Role Assignments      |
| User               | Individual participant in the migration program   | Has many Role Assignments and Approvals     |
| Constructed Table   | Curated supplemental dataset used during execution | Belongs to Execution Context; has Fields, Data, Approvals |
| Constructed Field   | Column metadata describing constructed table structure | Belongs to Constructed Table                  |
| Constructed Data    | Row-level JSON payload for enrichment/reference use | Belongs to Constructed Table                  |
| Constructed Table Approval | Governance sign-off for constructed data usage       | Belongs to Constructed Table & User           |
| Process Area Role Assignment | RBAC link tying users and roles to a process area   | Belongs to Process Area, Role, and User       |
| Process Area       | Functional domain for migrated data              | Belongs to Project; has many Data Objects   |
| Data Object        | Logical data set within a Process Area           | Belongs to Process Area; links to Systems   |
| System             | Source or target application or platform         | Has many Tables; links to Data Objects      |
| Table              | Physical table or view in a System               | Belongs to System; has many Fields          |
| Table Load Order   | Sequencing metadata for tables within a Data Object | Belongs to Data Object & Table; optionally has Approvals |
| Table Load Order Approval | Governance decision on a proposed table sequence       | Belongs to Table Load Order & User          |
| Field              | Column-level metadata for a Table                | Belongs to Table                            |
| Data Object System | Association between Data Object and System role  | Belongs to Data Object & System             |
| Field Load         | Controls per-release field ingestion preferences | Belongs to Release & Field; optional User   |
| Pre-Load Validation Result | Aggregated pre-load validation metrics            | Belongs to Release; has many Pre-Load Issues |
| Pre-Load Validation Issue | Individual pre-load validation finding             | Belongs to Pre-Load Validation Result       |
| Pre-Load Validation Approval | Governance sign-off for pre-load results           | Belongs to Pre-Load Validation Result & User |
| Post-Load Validation Result | Aggregated post-load validation metrics          | Belongs to Release; has many Post-Load Issues |
| Post-Load Validation Issue | Individual post-load validation finding           | Belongs to Post-Load Validation Result      |
| Post-Load Validation Approval | Governance sign-off for post-load results          | Belongs to Post-Load Validation Result & User |
| System Connection    | Configuration for accessing an external system       | Belongs to System                           |
| Ingestion Job        | Execution record for moving data for a table         | Belongs to Execution Context & Table        |

## Development Notes

- Timestamps are stored in UTC and automatically maintained via SQLAlchemy defaults.
- Deleting a parent entity cascades to its children via `ON DELETE CASCADE`.
- The test suite overrides the database dependency to remain self-contained and fast.

## Next Steps

- Add authentication/authorization around endpoints.
- Introduce pagination and filtering for list endpoints.
- Expand validation and business rules per organization needs.
- Add lineage reporting between systems and objects.
