# dbt Canvas Model Upgrade – Baseline Notes

## Current Table Relationship Builder Graph

The table relationship builder lives in `frontend/src/components/data-definition/DataDefinitionRelationshipBuilder.tsx` and is fed by a `DataDefinition` payload returned from `/data-definitions/{id}`. The relevant domain objects are defined in `frontend/src/types/data.ts`.

### Source Data Structures
- **DataDefinition**: includes `tables: DataDefinitionTable[]` and `relationships: DataDefinitionRelationship[]`, plus references to the owning system, data object, and metadata such as description.
- **DataDefinitionTable**: identifies the physical Databricks table via `tableId`, holds alias/description/load order flags, and embeds the full `Table` record (`schemaName`, `physicalName`, `systemId`, etc.). Each table carries `fields: DataDefinitionField[]` describing the definition-specific field configuration (notes, uniqueness, order) alongside the underlying `Field` metadata (type, description, flags).
- **DataDefinitionRelationship**: persists joins between definition tables. It stores IDs for the primary/foreign tables and fields, the join type (`inner`, `left`, `right`), and optional notes. The API also hydrates `primaryField`/`foreignField` with the corresponding `DataDefinitionField` objects when available.

### React Flow Node Payload
- Every definition table becomes a React Flow node with `id = table.id`.
- Node `data` mirrors `ReportTableNodeData`:
  - `tableId`, `label` (alias or table name), and `subtitle` (schema-qualified table name).
  - `fields`: array of `{ id, name, type, description }` using the definition-field ID and associated field metadata.
  - Interaction callbacks (`onFieldDragStart`, `onFieldJoin`, etc.) enable drag-to-join UX but are not persisted.
  - `selectedFieldIds` highlights fields currently chosen in the join dialog.
- Layout is generated on the fly. Nodes are arranged in a derived grid using `horizontalGap = 360` and `verticalGap = 360`. No positions are saved back to the server; coordinates are recomputed on every render and lost between sessions.
- Node dimensions are fixed (`width = 260`, `height = 320`). There is no concept of node grouping, annotations, or role/type metadata beyond what can be inferred from `DataDefinitionTable` (e.g., `isConstruction`).

### React Flow Edge Payload
- Each relationship becomes an edge with `id = relationship.id`.
- `source` is the **foreign** table ID, `target` is the **primary** table ID. Handles are positioned per field (`sourceHandle = source:${foreignField.id}`, `targetHandle = target:${primaryField.id}`).
- `edge.data` includes `relationshipId`, `primaryFieldId`, `foreignFieldId`, `joinType`, and `notes`.
- Styling is derived from join type (color-coded). Additional semantics—join condition SQL, relationship cardinality, filters—are not captured.
- Edges are interactive for editing/deleting via the dialog but there is no persisted metadata beyond what is stored in the relationship record.

### Notable Gaps for Canvas
- Node coordinates and viewport state are transient; dbt Canvas requires persistent `position: { x, y }` per node.
- Nodes lack explicit typing (source vs. model layers) or transformation metadata (materialization, tags, column tests) that dbt Canvas expects to drive the DAG and configuration panels.
- Edges only encode join type; Canvas differentiates dependency edges (`ref`, `source`, `exposure`) and may require directional semantics independent of join cardinality.
- There is no storage for per-node SQL, descriptions, owner info, or arbitrary attributes that map to dbt’s YAML/SQL configuration files.

## Metadata Needed for dbt Canvas Output

To translate the relationship builder into a dbt Canvas project we will need to enrich the payload with:

1. **Node Identity & Typing**
   - Stable node IDs (likely aligned with dbt resource names).
   - Resource type (`source`, `model`, `exposure`, etc.) and optional layer (staging/intermediate/mart).
   - Optional grouping/section identifiers if we plan to mirror Canvas swimlanes.

2. **Coordinates & Layout**
   - Persisted `{ x, y }` for each node (possibly per zoom level) so Canvas can render the DAG consistently.
   - Optional size overrides if we allow node resizing in the future.

3. **Transformation Metadata**
   - Materialization strategy, tags, owner, description.
   - SQL/config references (path to model file, custom config blocks).
   - Field-level test settings (unique, not_null, accepted_values) that will later translate into `schema.yml` tests.

4. **Edge Semantics**
   - Dependency type: `ref`, `source`, `exposure`, or possibly `metric` link.
   - Join context (optional) to seed model SQL: which columns join, join type, filters.
   - Ability to flag optional/non-primary dependencies (e.g., exposures that do not affect SQL generation).

5. **Global Context**
   - Target dbt project metadata (project name, profile target, model directories).
   - Mapping from data definition/system IDs to dbt packages or schemas when generating sources.

These requirements will guide subsequent tasks (exporter, translator, TestGen integration) and highlight which additional UI controls and persistence fields must be introduced.

## dbt Canvas JSON Schema Reference

The dbt Cloud Canvas export is a JSON document with the following top-level structure (captured from dbt Cloud sample exports and public documentation):

```json
{
   "metadata": {
      "project_id": "uuid",
      "name": "Canvas Name",
      "description": "optional",
      "dbt_version": "1.8.4",
      "generated_at": "2024-08-20T19:40:32.184142Z"
   },
   "nodes": [
      {
         "id": "node_id",
         "type": "model", // model | source | seed | snapshot | exposure | metric | group
         "name": "orders",
         "label": "Orders",
         "description": "optional",
         "path": "models/staging/stg_orders.sql", // optional for sources/exposures
         "materialization": "incremental",
         "config": {
            "tags": ["staging"],
            "meta": { "owner": "finance" },
            "tests": {
               "columns": {
                  "order_id": ["unique", "not_null"],
                  "status": [
                     {
                        "accepted_values": { "values": ["open", "closed", "pending"] }
                     }
                  ]
               }
            }
         },
         "position": { "x": 144, "y": 288 },
         "size": { "width": 256, "height": 144 },
         "group": "staging" // optional grouping bucket
      }
   ],
   "edges": [
      {
         "id": "edge_uuid",
         "source": "source_id",
         "target": "target_id",
         "type": "ref", // ref | source | exposure | metric
         "meta": {
            "join": {
               "type": "left",
               "condition": "orders.customer_id = customers.id"
            }
         }
      }
   ],
   "groups": [
      {
         "id": "staging",
         "label": "Staging",
         "color": "#2B6CB0"
      }
   ]
}
```

Key takeaways:

- **Metadata block** is optional but useful for stamping project identifiers, version, and timestamps.
- **Nodes** demand persisted `position` (Canvas renders exactly where saved) and allow optional `size`; we can mirror our React Flow coordinates directly.
- `type` governs Canvas iconography and how dbt interprets edges. We will likely map constructed/source tables to `source`, derived tables to `model`, and potential downstream dashboards to `exposure`.
- `config` is free-form but should align with dbt `config`/`schema.yml` structure so the translator can hydrate actual dbt files.
- **Edges** are directional: `source -> target`. Dependency types determine how dbt resolves macros (`ref` vs `source`). We can embed join metadata in `meta.join` to feed SQL scaffolding.
- **Groups** provide swimlane-like grouping. Not required, but nice for representing layers (sources / staging / marts).

This schema informs the shared TypeScript interface we will define in task 3 and the translator we will build later. For now we have the reference snapshot saved to drive modelling decisions.

## Shared Canvas Graph Interface (Task 3)

- Added `frontend/src/types/canvasGraph.ts` to centralize the contract exchanged between the React Flow builder and the backend translator. The interface mirrors the Canvas export shape while preserving Conversion Central IDs for round-tripping.
- **Nodes** – `CanvasGraphNode` captures `resourceType`, `layer`, persisted `position`, optional `size`, and an `origin` block for linking back to `DataDefinitionTable`/system metadata. The nested `config` object holds dbt-aligned settings (materialization, tags, owner, hooks, incremental config, tests, exposures, metrics) and column-level descriptors.
- **Edges** – `CanvasGraphEdge` standardizes dependency direction, dependency `type`, and optional join context (`join.type`, `condition`, field bindings, filters) so we can seed SQL later without losing relationship semantics.
- **Groups & Metadata** – `CanvasGraphGroup` and `CanvasGraphMetadata` keep swimlane layering, color, and project identifiers alongside generated-at/version info. A `warnings` array on `CanvasGraph` gives us a place to surface export-time notices to the UI.
- Task 4 will bind these types into React Flow state; Task 5 will surface editing controls for each `config` field. The backend translator will consume the same structures when generating dbt artifacts and TestGen seeds.

## React Flow Canvas State (Task 4)

- `DataDefinitionRelationshipBuilder` now keeps `nodeCanvasSettings` and `edgeCanvasSettings` alongside React Flow state so dbt metadata persists with the layout. Each node injects a `canvas` payload into `ReportTableNodeData`, mirroring `CanvasGraphNode` defaults (resource type, layer, description, config lists, column metadata, origin identifiers).
- Edge state captures dependency semantics using the shared types—`data.canvas` records the Canvas edge type (`ref` vs. `source`), join definition (columns, join type, condition), and links back to relationship notes via description/meta.
- Helper utilities reconcile API payloads with Canvas-friendly structures: we stabilise serialization, merge existing overrides, prune removed tables/relationships, and track default materialisations/tags/tests for future editing controls.
- React Flow nodes retain manual positions/resizes, while the canvas settings maps update incrementally and drop stale entries when tables or relationships are removed. These structures will feed the exporter/translator work in upcoming tasks.

## dbt Execution Prerequisites

- We will standardise on the open-source `dbt-core` CLI plus the `dbt-databricks` adapter; no dbt Cloud/Fusion components are required for this workflow.
- All compilation, parsing, and testing initiated by the translator or CI must target the existing Databricks SQL Warehouse. The generated `profiles.yml` entries should pull host/http_path/token from `DatabricksSqlSettings` and run exclusively against that warehouse.
- Capture these requirements as part of the automation in the upcoming translator tasks so local developers and CI runners bootstrap the same environment before running `dbt parse`/`dbt run`.
