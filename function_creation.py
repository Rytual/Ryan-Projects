# Create the clean_string function
def clean_string(text):
  
  # Replace spaces with underscores
  no_spaces = text.replace(" ", "_")
  
  # Convert to lowercase
  clean_text = no_spaces.lower()
  
  # Display the final text as an output
  return clean_text

converted_text = clean_string("I LoVe dATaCamP!")
print(converted_text)