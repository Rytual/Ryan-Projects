from collections import Counter
import statistics

def calculate_stats(numbers):
    mean = sum(numbers) / len(numbers)
    median = statistics.median(numbers)
    mode = Counter(numbers).most_common(1)[0][0]
    return mean, median, mode

# Example usage:
numbers = [1, 2, 3, 4, 5, 5, 6, 6, 6]
mean, median, mode = calculate_stats(numbers)
print("Mean:", mean)
print("Median:", median)
print("Mode:", mode)