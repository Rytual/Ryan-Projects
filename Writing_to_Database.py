import pandas as pd
from sqlalchemy import create_engine

# Database connection parameters
db_connection_string = 'mysql+pymysql://user:password@localhost/dbname'
engine = create_engine(db_connection_string)

# Sample data
data = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})

# Write to database
data.to_sql('my_table', engine, if_exists='replace', index=False)
print("Data written to database successfully.")
