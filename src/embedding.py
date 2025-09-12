import ollama
import re


def preprocess_text(text):
    # Remove \n and replace with spaces
    text = text.replace('\n', ' ')
    # Remove unicode spaces like \u202f
    text = re.sub(r'[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]', ' ', text)
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    # Strip leading/trailing whitespace
    return text.strip()

def embed(text)->list[list[float]]:

    if isinstance(text,list):
        input = [preprocess_text(t) for t in text]
    else:
        input = preprocess_text(text)

    return ollama.embed(model = 'nomic-embed-text', input = input)['embeddings']

