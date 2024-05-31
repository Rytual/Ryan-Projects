import pyodbc
import pandas as pd

# Database connection parameters
server = 'localhost'
database = 'AdventureWorksDW2022'
driver = '{ODBC Driver 17 for SQL Server}'

# SQL query
sql_query = "SELECT * FROM DimCurrency"

# Connect to SQL Server and fetch data
conn = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes;')
data = pd.read_sql(sql_query, conn)
conn.close()

# Display data
print(data.head())
