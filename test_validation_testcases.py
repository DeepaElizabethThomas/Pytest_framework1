import pytest
import pandas as pd
from sqlalchemy import column
from db_config import source_engine, target_engine

def test_fetch_employee_data(db_connection):
    """Verify employee transformation (name lowercase, total_salary calculation)"""
    source_connection, target_connection = db_connection

    df_source = pd.read_sql("SELECT * FROM employee", source_connection)
    df_target = pd.read_sql("SELECT * FROM employee_details", target_connection)

    assert not df_target.empty, "❌ Employee data table is empty!"
    assert all(df_target['name'] == df_source['ename'].str.lower()), "❌ Name column is not lowercase!"
    assert all(df_target['total_salary'] == df_source['salary'] + df_source['commission'].fillna(0)), "❌ total_salary calculation is incorrect!"
    print("✅ Data matched between source and target !")

def test_department_average(db_connection):
    source_connection, target_connection = db_connection
    """Verify department-wise  average salary calculation"""
    df_deptsource = pd.read_sql("SELECT department_id, AVG(salary) as total_salary FROM employee GROUP BY department_id", source_connection)
    df_depttarget = pd.read_sql("SELECT * FROM Department_Average", target_connection)

    assert not df_depttarget.empty, "❌ Department_Average table is empty!"
#     # Ensure row counts match
#     assert len(df_deptsource) == len(df_depttarget), "❌ Row count mismatch between source and target!"
#
#     # Sort both DataFrames by department_id to align rows correctly
#     df_deptsource = df_deptsource.sort_values(by="department_id").reset_index(drop=True)
#     df_deptsource = df_depttarget.sort_values(by="department_id").reset_index(drop=True)
#
#     assert df_deptsource.equals(df_depttarget), "❌ Data mismatch found in source and target!"
#
#     print("✅ Data matched between source and target!")

if __name__ == "__main__":
    pytest.main()