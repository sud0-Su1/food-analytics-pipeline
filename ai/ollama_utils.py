import os

import ollama
from dotenv import load_dotenv

load_dotenv()

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:latest")
OLLAMA_EMBEDDING_MODEL = os.getenv("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_BATCH_SIZE = 32

client = ollama.Client(host=OLLAMA_HOST)


def embed_texts(texts):
    vectors = []
    for start in range(0, len(texts), EMBEDDING_BATCH_SIZE):
        batch = [str(text) if text is not None else "" for text in texts[start:start + EMBEDDING_BATCH_SIZE]]
        response = client.embed(model=OLLAMA_EMBEDDING_MODEL, input=batch)
        vectors.extend(response.embeddings)
    return vectors
