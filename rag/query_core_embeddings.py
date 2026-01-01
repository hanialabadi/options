from pathlib import Path
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

EMB_PATH = Path("/Users/haniabadi/Documents/Github/options/embeddings/core/core_embeddings.pkl")

# Load embeddings
with open(EMB_PATH, "rb") as f:
    data = pickle.load(f)

embeddings = data["embeddings"]
chunks = data["chunks"]
sources = data["source"]

# Load model (same as embedding)
model = SentenceTransformer("all-MiniLM-L6-v2")

def cosine_sim(a, b):
    a = a / np.linalg.norm(a)
    b = b / np.linalg.norm(b)
    return np.dot(a, b)

def query(text, k=5):
    q_emb = model.encode([text])[0]
    sims = [cosine_sim(q_emb, emb) for emb in embeddings]
    top_idx = np.argsort(sims)[-k:][::-1]

    results = []
    for i in top_idx:
        results.append(
            {
                "score": float(sims[i]),
                "source": sources[i],
                "text": chunks[i][:600]
            }
        )
    return results

if __name__ == "__main__":
    q = "How implied volatility affects option pricing and risk"
    results = query(q, k=5)

    print(f"\nQUERY: {q}\n")
    for r in results:
        print("=" * 80)
        print(f"Score: {r['score']:.4f}")
        print(f"Source: {r['source']}")
        print(r["text"])
