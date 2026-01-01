# rag/ask.py
import sys
from query_core_embeddings import query

query_text = sys.argv[1]
results = query(query_text, k=5)

with open("rag/context/latest_context.md", "w") as f:
    f.write(f"# RAG Context\n\n## Query\n{query_text}\n\n")
    for r in results:
        f.write(f"### Source: {r['source']}\n")
        f.write(r["text"] + "\n\n")
