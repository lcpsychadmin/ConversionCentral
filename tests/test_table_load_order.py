from http import HTTPStatus

from app.models import DataObject, ProcessArea, System, Table, TableLoadOrder
from app.services import DAGBuilder


def _create_project_hierarchy(client) -> str:
    process_area_id = client.post(
        "/process-areas",
        json={
            "name": "Core",
            "description": "",
            "status": "draft",
        },
    ).json()["id"]

    data_object_id = client.post(
        "/data-objects",
        json={
            "process_area_id": process_area_id,
            "name": "Customers",
            "description": "",
            "status": "draft",
        },
    ).json()["id"]

    return data_object_id


def _create_system(client) -> str:
    return client.post(
        "/systems",
        json={
            "name": "Ordering",
            "physical_name": "SYS_ORDERING",
            "description": "",
            "status": "active",
        },
    ).json()["id"]


def _create_table(client, system_id: str, name: str, physical_name: str) -> str:
    return client.post(
        "/tables",
        json={
            "system_id": system_id,
            "name": name,
            "physical_name": physical_name,
            "schema_name": "dbo",
            "status": "active",
        },
    ).json()["id"]


def _create_user(client) -> str:
    return client.post(
        "/users",
        json={
            "name": "Reviewer",
            "email": "reviewer@example.com",
            "status": "active",
        },
    ).json()["id"]


def test_table_load_order_crud_enforces_contiguous_sequences(client):
    data_object_id = _create_project_hierarchy(client)
    system_id = _create_system(client)
    table_orders_id = _create_table(client, system_id, "Orders", "dbo.orders")
    table_customers_id = _create_table(client, system_id, "Customers", "dbo.customers")

    invalid_resp = client.post(
        "/table-load-orders",
        json={
            "data_object_id": data_object_id,
            "table_id": table_orders_id,
            "sequence": 2,
        },
    )
    assert invalid_resp.status_code == HTTPStatus.BAD_REQUEST

    first_resp = client.post(
        "/table-load-orders",
        json={
            "data_object_id": data_object_id,
            "table_id": table_orders_id,
            "sequence": 1,
            "notes": "Primary load",
        },
    )
    assert first_resp.status_code == HTTPStatus.CREATED
    first_order = first_resp.json()
    assert first_order["sequence"] == 1

    second_resp = client.post(
        "/table-load-orders",
        json={
            "data_object_id": data_object_id,
            "table_id": table_customers_id,
            "sequence": 2,
        },
    )
    assert second_resp.status_code == HTTPStatus.CREATED
    second_order = second_resp.json()
    assert second_order["sequence"] == 2

    reorder_resp = client.put(
        f"/table-load-orders/{second_order['id']}",
        json={"sequence": 1},
    )
    assert reorder_resp.status_code == HTTPStatus.OK
    assert reorder_resp.json()["sequence"] == 1

    list_resp = client.get("/table-load-orders")
    assert list_resp.status_code == HTTPStatus.OK
    sequences = [order["sequence"] for order in list_resp.json()]
    assert sequences == [1, 2]
    assert list_resp.json()[0]["table_id"] == second_order["table_id"]

    delete_resp = client.delete(f"/table-load-orders/{first_order['id']}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT

    post_delete_list = client.get("/table-load-orders").json()
    assert len(post_delete_list) == 1
    assert post_delete_list[0]["sequence"] == 1

    duplicate_table_resp = client.post(
        "/table-load-orders",
        json={
            "data_object_id": data_object_id,
            "table_id": table_customers_id,
            "sequence": 1,
        },
    )
    assert duplicate_table_resp.status_code == HTTPStatus.BAD_REQUEST


def test_table_load_order_approval_crud(client):
    data_object_id = _create_project_hierarchy(client)
    system_id = _create_system(client)
    table_id = _create_table(client, system_id, "Ledger", "dbo.ledger")
    order_id = client.post(
        "/table-load-orders",
        json={
            "data_object_id": data_object_id,
            "table_id": table_id,
            "sequence": 1,
        },
    ).json()["id"]
    user_id = _create_user(client)

    create_resp = client.post(
        "/table-load-order-approvals",
        json={
            "table_load_order_id": order_id,
            "approver_id": user_id,
            "role": "sme",
            "decision": "approved",
            "comments": "Looks good",
        },
    )
    assert create_resp.status_code == HTTPStatus.CREATED
    approval_id = create_resp.json()["id"]

    update_resp = client.put(
        f"/table-load-order-approvals/{approval_id}",
        json={"decision": "rejected", "comments": "Needs revision"},
    )
    assert update_resp.status_code == HTTPStatus.OK
    body = update_resp.json()
    assert body["decision"] == "rejected"
    assert body["comments"] == "Needs revision"

    duplicate_resp = client.post(
        "/table-load-order-approvals",
        json={
            "table_load_order_id": order_id,
            "approver_id": user_id,
            "role": "sme",
            "decision": "approved",
        },
    )
    assert duplicate_resp.status_code == HTTPStatus.BAD_REQUEST

    delete_resp = client.delete(f"/table-load-order-approvals/{approval_id}")
    assert delete_resp.status_code == HTTPStatus.NO_CONTENT


def test_dag_builder_respects_table_load_order(db_session):
    process_area = ProcessArea(name="Finance", description="", status="draft")
    data_object = DataObject(name="Ledger", description="", status="draft", process_area=process_area)

    system = System(
        name="ERP",
        physical_name="SYS_ERP",
        description="",
        status="active",
    )

    table_orders = Table(
        system=system,
        name="Orders",
        physical_name="dbo.orders",
        schema_name="dbo",
        description="",
        status="active",
    )
    table_customers = Table(
        system=system,
        name="Customers",
        physical_name="dbo.customers",
        schema_name="dbo",
        description="",
        status="active",
    )

    load_order_one = TableLoadOrder(
        data_object=data_object,
        table=table_customers,
        sequence=2,
    )
    load_order_two = TableLoadOrder(
        data_object=data_object,
        table=table_orders,
        sequence=1,
    )

    db_session.add_all([
        process_area,
        data_object,
        system,
        table_orders,
        table_customers,
        load_order_one,
        load_order_two,
    ])
    db_session.commit()

    dag_builder = DAGBuilder(db_session)
    table_order = dag_builder.build_table_order()

    assert [entry["id"] for entry in table_order] == [table_orders.id, table_customers.id]
    assert table_order[0]["order"] == 0
    assert table_order[1]["order"] == 1
