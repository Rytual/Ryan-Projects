# Define clean_text
def clean_text(text, lower=True):
  if lower == False:
    clean_text = text.replace("upper", "lower")
    return clean_text
  else:
    clean_text = text.replace("text", "remove")
    clean_text = clean_text.lower()
    return clean_text