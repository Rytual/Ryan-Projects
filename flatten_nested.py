def flatten_list(nested_list):
    flattened_list = []
    for sublist in nested_list:
        if isinstance(sublist, list):
            flattened_list.extend(flatten_list(sublist))
        else:
            flattened_list.append(sublist)
    return flattened_list

# Example usage:
nested_list = [[1, 2], [3, [4, 5]], [6, [7, 8, [9, 10]]]]
flattened_list = flatten_list(nested_list)
print("Flattened List:", flattened_list)