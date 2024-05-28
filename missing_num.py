def find_missing_number(numbers):
    n = len(numbers) + 1
    expected_sum = n * (n + 1) // 2
    actual_sum = sum(numbers)
    missing_number = expected_sum - actual_sum
    return missing_number

# Example usage:
numbers = [1, 2, 3, 4, 6, 7, 8, 9, 10]
missing_number = find_missing_number(numbers)
print("Missing Number:", missing_number)