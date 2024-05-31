import json

# Sample data
data = {
    'name': 'John',
    'age': 30,
    'city': 'New York'
}

# Write to JSON file
with open('data.json', 'w') as f:
    json.dump(data, f)

# Read from JSON file
with open('data.json', 'r') as f:
    data_loaded = json.load(f)

print(data_loaded)
