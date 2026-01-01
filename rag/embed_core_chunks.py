from pathlib import Path
import pickle
from sentence_transformers import SentenceTransformer

CHUNK_DIR = Path("/Users/haniabadi/Documents/Github/options/chunks/core")
OUT_PATH = Path("/Users/haniabadi/Documents/Github/options/embeddings/core/core_embeddings.pkl")

model = SentenceTransformer("all-MiniLM-L6-v2")

all_chunks = []
metadata = []

for chunk_file in CHUNK_DIR.glob("*_chunks.txt"):
    source = chunk_file.name
    text = chunk_file.read_text(encoding="utf-8")

    chunks = [c.strip() for c in text.split("[CHUNK") if c.strip()]

    for c in chunks:
        all_chunks.append(c)
        metadata.append(source)

print(f"Total chunks to embed: {len(all_chunks)}")

embeddings = model.encode(all_chunks, show_progress_bar=True)

with open(OUT_PATH, "wb") as f:
    pickle.dump(
        {
            "embeddings": embeddings,
            "chunks": all_chunks,
            "source": metadata,
        },
        f
    )

print(f"Embeddings saved to {OUT_PATH}")
