"""
query_core_embeddings.py — Semantic search over options knowledge base

Automatically selects the best available embedding index:
  1. BAAI/bge-base-en-v1.5 (768-dim, preferred — higher semantic fidelity)
  2. all-MiniLM-L6-v2      (384-dim, fallback — smaller, general-purpose)

Usage:
    from knowledge.rag.query_core_embeddings import query

    results = query("short gamma position when theta carry is exceeded by gamma drag", k=5)
    for r in results:
        print(r['score'], r['source'])
        print(r['text'])
"""

from pathlib import Path
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer
from functools import lru_cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Embedding index paths — BGE preferred, MiniLM fallback
_BGE_PATH  = PROJECT_ROOT / "knowledge" / "embeddings" / "core" / "core_embeddings_bge.pkl"
_MINI_PATH = PROJECT_ROOT / "knowledge" / "embeddings" / "core" / "core_embeddings.pkl"

# Legacy path used by old embed_core_chunks.py
_LEGACY_PATH = PROJECT_ROOT / "embeddings" / "core" / "core_embeddings.pkl"


def _select_index() -> tuple[Path, str]:
    """Return (pkl_path, model_name) for best available index."""
    if _BGE_PATH.exists():
        return _BGE_PATH, "BAAI/bge-base-en-v1.5"
    if _MINI_PATH.exists():
        return _MINI_PATH, "all-MiniLM-L6-v2"
    if _LEGACY_PATH.exists():
        return _LEGACY_PATH, "all-MiniLM-L6-v2"
    raise FileNotFoundError(
        "No embedding index found. Run knowledge/rag/embed_bge.py to build one."
    )


@lru_cache(maxsize=1)
def _load_index():
    """Load embedding index and model — cached for process lifetime."""
    idx_path, model_name = _select_index()
    with open(idx_path, "rb") as f:
        data = pickle.load(f)
    embeddings = np.array(data["embeddings"], dtype=np.float32)
    chunks     = data["chunks"]
    sources    = data["source"]
    # Normalize once at load time (BGE may already be normalized; safe to re-normalize)
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1.0, norms)
    embeddings = embeddings / norms

    model = SentenceTransformer(model_name)
    is_bge = "bge" in model_name.lower()

    return embeddings, chunks, sources, model, is_bge, model_name, str(idx_path)


def query(text: str, k: int = 5) -> list[dict]:
    """
    Semantic search over the options knowledge base.

    Parameters
    ----------
    text : str
        Natural-language query describing the trading situation or concept.
    k : int
        Number of top results to return.

    Returns
    -------
    list of dict with keys:
        score   float   cosine similarity (higher = more relevant)
        source  str     book/source title
        text    str     passage text (up to 600 chars)
        model   str     embedding model used
    """
    embeddings, chunks, sources, model, is_bge, model_name, idx_path = _load_index()

    # BGE recommends query prefix for retrieval tasks
    query_text = f"Represent this sentence for searching relevant passages: {text}" if is_bge else text

    q_emb = model.encode([query_text], normalize_embeddings=True)[0].astype(np.float32)

    # Vectorised cosine similarity (embeddings already normalised)
    sims = embeddings @ q_emb
    top_idx = np.argsort(sims)[-k:][::-1]

    return [
        {
            "score":  float(sims[i]),
            "source": sources[i],
            "text":   chunks[i][:600],
            "model":  model_name,
        }
        for i in top_idx
    ]


def query_info() -> dict:
    """Return metadata about the currently loaded index."""
    embeddings, chunks, sources, model, is_bge, model_name, idx_path = _load_index()
    return {
        "model":     model_name,
        "n_chunks":  len(chunks),
        "n_sources": len(set(sources)),
        "dim":       embeddings.shape[1],
        "index":     idx_path,
    }


if __name__ == "__main__":
    import sys
    q_text = sys.argv[1] if len(sys.argv) > 1 else \
        "short gamma covered call when gamma drag exceeds theta carry exit or roll decision"

    info = query_info()
    print(f"\nIndex: {info['model']}  |  {info['n_chunks']:,} chunks  |  {info['n_sources']} sources  |  dim={info['dim']}")
    print(f"Query: {q_text}\n")

    results = query(q_text, k=5)
    for i, r in enumerate(results, 1):
        print(f"{'='*80}")
        print(f"#{i}  Score: {r['score']:.4f}  |  {r['source'][:70]}")
        print(r['text'][:500])
        print()
