"""
embed_bge.py — Re-embed all chunks with BAAI/bge-base-en-v1.5

Reads from BOTH chunk directories:
  - chunks/core/             (9 core options books: Natenberg, McMillan, Hull, etc.)
  - knowledge/chunks/core/   (27 extended library)

Writes to:
  knowledge/embeddings/core/core_embeddings_bge.pkl

The existing MiniLM pkl is untouched. query_core_embeddings.py will be
updated to prefer the BGE pkl when available.

Usage:
    python knowledge/rag/embed_bge.py
"""

from pathlib import Path
import pickle
import time
from sentence_transformers import SentenceTransformer

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

CHUNK_DIRS = [
    PROJECT_ROOT / "chunks" / "core",
    PROJECT_ROOT / "knowledge" / "chunks" / "core",
]

OUT_PATH = PROJECT_ROOT / "knowledge" / "embeddings" / "core" / "core_embeddings_bge.pkl"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

MODEL_NAME = "BAAI/bge-base-en-v1.5"

# ── Collect all chunks ────────────────────────────────────────────────────────
all_chunks: list[str] = []
all_sources: list[str] = []
seen_files: set[str] = set()

for chunk_dir in CHUNK_DIRS:
    if not chunk_dir.exists():
        print(f"[SKIP] Dir not found: {chunk_dir}")
        continue
    files = sorted(chunk_dir.glob("*_chunks.txt"))
    print(f"[DIR] {chunk_dir} — {len(files)} chunk files")
    for chunk_file in files:
        if chunk_file.name in seen_files:
            print(f"  [DUP] {chunk_file.name} — already loaded from other dir, skipping")
            continue
        seen_files.add(chunk_file.name)
        source = chunk_file.name.replace("_chunks.txt", "")
        text = chunk_file.read_text(encoding="utf-8", errors="replace")
        chunks = [c.strip() for c in text.split("[CHUNK") if c.strip()]
        for c in chunks:
            all_chunks.append(c)
            all_sources.append(source)
        print(f"  {chunk_file.name[:70]:70s}  {len(chunks):5d} chunks")

print(f"\nTotal chunks to embed: {len(all_chunks)}")
print(f"Unique sources:        {len(seen_files)}")
print(f"Output:                {OUT_PATH}")
print()

# ── Load model ────────────────────────────────────────────────────────────────
print(f"Loading model: {MODEL_NAME} ...")
t_load = time.time()
model = SentenceTransformer(MODEL_NAME)
print(f"Model loaded in {time.time() - t_load:.1f}s")
print(f"Embedding dim: {model.get_sentence_embedding_dimension()}")
print()

# ── Embed ─────────────────────────────────────────────────────────────────────
# BGE works best with normalize_embeddings=True for cosine similarity
print("Embedding chunks (batch_size=64, normalize=True)...")
t_embed = time.time()
embeddings = model.encode(
    all_chunks,
    batch_size=64,
    normalize_embeddings=True,
    show_progress_bar=True,
    convert_to_numpy=True,
)
elapsed = time.time() - t_embed
print(f"\nEmbedding complete in {elapsed:.1f}s ({elapsed/60:.1f} min)")
print(f"Embedding shape: {embeddings.shape}")

# ── Save ──────────────────────────────────────────────────────────────────────
print(f"\nSaving to {OUT_PATH} ...")
with open(OUT_PATH, "wb") as f:
    pickle.dump(
        {
            "embeddings": embeddings,
            "chunks": all_chunks,
            "source": all_sources,
            "model": MODEL_NAME,
            "dim": int(embeddings.shape[1]),
            "n_chunks": len(all_chunks),
        },
        f,
        protocol=pickle.HIGHEST_PROTOCOL,
    )
print(f"Done. {len(all_chunks):,} chunks saved.")
