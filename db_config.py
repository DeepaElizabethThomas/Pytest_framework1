from sqlalchemy import create_engine

# Define connection parameters
SERVER = r'localhost\SQLEXPRESS'
SOURCE_DB = 'SourceDb'
TARGET_DB = 'TargetDb'

# Create engine for source and target databases
source_engine = create_engine(f'mssql+pyodbc://{SERVER}/{SOURCE_DB}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
target_engine = create_engine(f'mssql+pyodbc://{SERVER}/{TARGET_DB}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')