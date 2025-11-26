from __future__ import annotations

import io
import json

from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import DataDefinitionTable, DataObject, DataObjectSystem, Field, ProcessArea, System, Table

from app.ingestion.engine import get_ingestion_engine, reset_ingestion_engine

from openpyxl import Workbook


def _make_file_tuple(filename: str, content: str) -> tuple[str, bytes, str]:
    return (filename, content.encode('utf-8'), 'text/csv')


def _make_excel_file_tuple(filename: str, rows: list[list[object]]) -> tuple[str, bytes, str]:
    workbook = Workbook()
    sheet = workbook.active
    for row in rows:
        sheet.append(row)

    buffer = io.BytesIO()
    workbook.save(buffer)
    workbook.close()
    return (
        filename,
        buffer.getvalue(),
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


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


def test_upload_preview_supports_excel_files(client: TestClient) -> None:
    reset_ingestion_engine()
    response = client.post(
        '/upload-data/preview',
        data={'has_header': 'true'},
        files={'file': _make_excel_file_tuple('sample.xlsx', [['name', 'age'], ['Alice', 30]])}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['total_rows'] == 1
    assert payload['columns'][0]['field_name'] == 'name'
    assert payload['columns'][1]['field_name'] == 'age'


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


def test_create_table_from_excel_upload(client: TestClient, db_session: Session) -> None:
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
        files={'file': _make_excel_file_tuple('people.xlsx', [['name', 'age'], ['Alice', 30]])}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['rows_inserted'] == 1
    assert payload['table_name'] == 'people_data'

    engine = get_ingestion_engine()
    with engine.connect() as connection:
        rows = connection.execute(text('SELECT name, age FROM "people_data"')).fetchall()
    assert rows == [('Alice', 30)]


def test_create_table_with_column_overrides(client: TestClient, db_session: Session) -> None:
    reset_ingestion_engine()
    ids = _seed_upload_dependencies(db_session)
    overrides = json.dumps(
        [
            {'field_name': 'name', 'target_name': 'player_name'},
            {'field_name': 'active', 'target_name': 'is_active', 'target_type': 'boolean'},
        ]
    )

    response = client.post(
        '/upload-data/create-table',
        data={
            'table_name': 'team data',
            'mode': 'create',
            'has_header': 'true',
            'product_team_id': ids['product_team_id'],
            'data_object_id': ids['data_object_id'],
            'system_id': ids['system_id'],
            'column_overrides': overrides,
        },
        files={'file': _make_file_tuple('team.csv', 'name,active\nAlice,1\nBob,0\n')}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload['rows_inserted'] == 2

    engine = get_ingestion_engine()
    with engine.connect() as connection:
        pragma = connection.execute(text("PRAGMA table_info('team_data')")).fetchall()
        column_names = [row[1] for row in pragma]
        assert 'player_name' in column_names
        assert 'is_active' in column_names

        rows = connection.execute(
            text('SELECT player_name, is_active FROM "team_data" ORDER BY player_name')
        ).fetchall()
    assert rows == [('Alice', 1), ('Bob', 0)]

    db_session.expire_all()
    field_names = {field.name for field in db_session.query(Field).all()}
    assert 'player_name' in field_names
    assert 'is_active' in field_names


def test_create_table_with_unknown_column_override(client: TestClient, db_session: Session) -> None:
    reset_ingestion_engine()
    ids = _seed_upload_dependencies(db_session)
    overrides = json.dumps([
        {'field_name': 'does_not_exist', 'target_name': 'renamed'}
    ])

    response = client.post(
        '/upload-data/create-table',
        data={
            'table_name': 'bad override',
            'mode': 'create',
            'has_header': 'true',
            'product_team_id': ids['product_team_id'],
            'data_object_id': ids['data_object_id'],
            'system_id': ids['system_id'],
            'column_overrides': overrides,
        },
        files={'file': _make_file_tuple('sample.csv', 'name\nAlice\n')}
    )
    assert response.status_code == 400
    assert 'unknown field' in response.json()['detail'].lower()


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
