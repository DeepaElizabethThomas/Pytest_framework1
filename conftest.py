# Pytest Fixture for Database Connection

import pytest
from sqlalchemy import create_engine

@pytest.fixture(scope="module")
def db_connection():
    """Setup and teardown for database connection"""
    server = r'localhost\SQLEXPRESS'  # local server
    source_db = 'SourceDb'
    target_db = 'TargetDb'


    engine_source = create_engine(f'mssql+pyodbc://{server}/{source_db}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
    engine_target = create_engine(f'mssql+pyodbc://{server}/{target_db}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

    source_connection = engine_source.connect()
    target_connection = engine_target.connect()

# Provide connection to test cases
    yield source_connection,target_connection

    # Teardown (closing connections)
    source_connection.close()
    target_connection.close()

    # Dispose engines
    engine_source.dispose()
    engine_target.dispose()

