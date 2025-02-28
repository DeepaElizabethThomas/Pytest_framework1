import pandas as pd
from db_config import source_engine, target_engine
import db_queries as queries
from sqlalchemy import text

def extract_data():
    """Extract employee data from SourceDB"""
    return pd.read_sql(queries.QUERY_EXTRACT_EMPLOYEES, source_engine)

def transform_data(df):
        """Transform data: Convert name to lowercase, calculate total_salary handling null commissions"""
        # Convert 'name' to lowercase
        df['ename'] = df['ename'].str.lower()

        # Replace null commissions with 0 before calculating total_salary
        df['commission'] = df['commission'].fillna(0)

        # Calculate total_salary
        df['total_salary'] = df['salary'] + df['commission']

        # Return the relevant columns
        return df[['ename', 'department_id', 'salary', 'commission', 'total_salary']]

def load_data(df):
    """Load transformed data into TargetDB"""
    with target_engine.begin() as conn:
        # Ensure table exists
        conn.execute(text(queries.QUERY_CREATE_EMPLOYEE_DETAILS))

        # Insert transformed data
        for _, row in df.iterrows():
            conn.execute(
                text(queries.QUERY_LOAD_EMPLOYEE_DETAILS),
                {
                    'name': row['ename'],  # ✅ Map 'ename' from DataFrame to 'name' in DB
                    'department_id': row['department_id'],
                    'salary': row['salary'],
                    'commission': row['commission'],
                    'total_salary': row['total_salary']
                }
            )

        # Execute additional queries
        conn.execute(text(queries.QUERY_CREATE_DEPARTMENT_AVERAGE))
        conn.execute(text(queries.QUERY_CALCULATE_DEPARTMENT_AVERAGE))

def run_etl():
    """Run the complete ETL process"""
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    print(transformed_data)
    load_data(transformed_data)
    print("✅ ETL Process Completed Successfully!")

if __name__ == "__main__":
    run_etl()
