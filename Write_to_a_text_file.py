# Write to a text file
with open('output.txt', 'w') as f:
    f.write("Hello, World!\n")
    f.write("This is a text file.")

# Read from a text file
with open('output.txt', 'r') as f:
    content = f.read()

print(content)
