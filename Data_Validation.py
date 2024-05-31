import pandas as pd

# Load data
data = pd.read_csv('data.csv')

# Validate data
assert data['column1'].notnull().all(), "Null values found in column1"
assert (data['column2'] > 0).all(), "Non-positive values found in column2"

print("Data validation passed.")
