import pandas as pd

# Load data
data = pd.read_csv('data.csv')

# Group by and aggregate
aggregated_data = data.groupby('category').agg({'sales': 'sum', 'profit': 'mean'})

# Display aggregated data
print(aggregated_data)
