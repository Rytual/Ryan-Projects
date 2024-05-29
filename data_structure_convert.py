# Create the convert_data_structure function
def convert_data_structure(data, data_type="list"):
  
  # If tuple, convert to a tuple
  if data_type == "tuple":
    data = tuple(data)
  
  # Else if set, convert to a set
  elif data_type == "set":
    data = set(data)
  else:
    data = list(data)
  return data

# Call the function
convert_data_structure({"a", 1, "b", 2, "c", 3}, data_type="set")