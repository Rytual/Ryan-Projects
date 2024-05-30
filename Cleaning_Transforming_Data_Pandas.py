import pandas as pd

# Load data
data = pd.read_csv('data.csv')

# Fill missing values
data.fillna(0, inplace=True)

# Remove duplicates
data.drop_duplicates(inplace=True)

# Convert column data types
data['date'] = pd.to_datetime(data['date'])

# Display cleaned data
print(data.head())
