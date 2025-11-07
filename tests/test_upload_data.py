from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import DataDefinitionTable, DataObject, DataObjectSystem, ProcessArea, System, Table

from app.ingestion.engine import get_ingestion_engine, reset_ingestion_engine


def _make_file_tuple(filename: str, content: str) -> tuple[str, bytes, str]:
    return (filename, content.encode('utf-8'), 'text/csv')


def _seed_upload_dependencies(session: Session) -> dict[str, str]:
    process_area = ProcessArea(name='Team Upload', status='active')
    system = System(name='Source System', physical_name='source_system', status='active')
    data_object = DataObject(process_area=process_area, name='Schedule', status='active')
    session.add_all([process_area, system, data_object])
    session.flush()

    link = DataObjectSystem(data_object_id=data_object.id, system_id=system.id, relationship_type='source')
    session.add(link)
    session.commit()

    return {
        'product_team_id': str(process_area.id),
        'data_object_id': str(data_object.id),
        'system_id': str(system.id),
    }


def test_upload_preview_returns_columns(client: TestClient) -> None:
    reset_ingestion_engine()
    response = client.post(
        '/upload-data/preview',
        data={'has_header': 'true'},
        files={'file': _make_file_tuple('sample.csv', 'name,age\nAlice,30\n')}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['total_rows'] == 1
    assert payload['columns'][0]['field_name'] == 'name'
    assert payload['columns'][1]['field_name'] == 'age'
    assert payload['columns'][1]['inferred_type'] == 'integer'


def test_create_table_from_upload(client: TestClient, db_session: Session) -> None:
    reset_ingestion_engine()
    ids = _seed_upload_dependencies(db_session)
    response = client.post(
        '/upload-data/create-table',
        data={
            'table_name': 'People Data',
            'mode': 'create',
            'has_header': 'true',
            'product_team_id': ids['product_team_id'],
            'data_object_id': ids['data_object_id'],
            'system_id': ids['system_id'],
        },
        files={'file': _make_file_tuple('people.csv', 'name,age\nAlice,30\nBob,25\n')}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['table_name'] == 'people_data'
    assert payload['rows_inserted'] == 2
    assert payload['table_id']

    engine = get_ingestion_engine()
    with engine.connect() as connection:
        count = connection.execute(text('SELECT COUNT(*) FROM "people_data"')).scalar()
    assert count == 2


def test_replace_table_from_upload(client: TestClient, db_session: Session) -> None:
    reset_ingestion_engine()
    ids = _seed_upload_dependencies(db_session)
    initial = client.post(
        '/upload-data/create-table',
        data={
            'table_name': 'events',
            'mode': 'create',
            'has_header': 'true',
            'product_team_id': ids['product_team_id'],
            'data_object_id': ids['data_object_id'],
            'system_id': ids['system_id'],
        },
        files={'file': _make_file_tuple('events.csv', 'title\nPractice\nMeeting\n')}
    )
    assert initial.status_code == 200, initial.text

    response = client.post(
        '/upload-data/create-table',
        data={
            'table_name': 'events',
            'mode': 'replace',
            'has_header': 'true',
            'product_team_id': ids['product_team_id'],
            'data_object_id': ids['data_object_id'],
            'system_id': ids['system_id'],
        },
        files={'file': _make_file_tuple('events.csv', 'title\nUpdated\n')}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['rows_inserted'] == 1

    engine = get_ingestion_engine()
    with engine.connect() as connection:
        rows = connection.execute(text('SELECT title FROM "events"')).fetchall()
    assert rows == [('Updated',)]


def test_delete_uploaded_table_removes_metadata_and_physical_table(
    client: TestClient,
    db_session: Session,
) -> None:
    reset_ingestion_engine()
    ids = _seed_upload_dependencies(db_session)
    response = client.post(
        '/upload-data/create-table',
        data={
            'table_name': 'people data',
            'mode': 'create',
            'has_header': 'true',
            'product_team_id': ids['product_team_id'],
            'data_object_id': ids['data_object_id'],
            'system_id': ids['system_id'],
        },
        files={'file': _make_file_tuple('people.csv', 'name,age\nAlice,30\n')}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    table_id = payload['table_id']
    assert table_id

    engine = get_ingestion_engine()
    with engine.connect() as connection:
        existing = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='people_data'")
        ).fetchall()
    assert existing

    delete_response = client.delete(f'/upload-data/tables/{table_id}')
    assert delete_response.status_code == 204, delete_response.text

    with engine.connect() as connection:
        tables = connection.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='people_data'")
        ).fetchall()
    assert tables == []

    db_session.expire_all()
    assert db_session.query(Table).count() == 0
    assert db_session.query(DataDefinitionTable).count() == 0
