"""
ingest_new_books.py — Add new PDFs to the RAG knowledge base.

Steps:
  1. Extract text from each new PDF (pypdf)
  2. Chunk into 800-char segments with 150-char overlap (langchain)
  3. Save chunk file to knowledge/chunks/core/
  4. Rebuild the full embeddings pkl from ALL chunk files

Usage:
    python knowledge/rag/ingest_new_books.py
"""

from pathlib import Path
from pypdf import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
import pickle

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parent.parent.parent
PDF_DIR     = ROOT / "core/_support/knowloadge"
CHUNK_DIR   = ROOT / "knowledge/chunks/core"
EMBED_PATH  = ROOT / "knowledge/embeddings/core/core_embeddings.pkl"

CHUNK_DIR.mkdir(parents=True, exist_ok=True)
EMBED_PATH.parent.mkdir(parents=True, exist_ok=True)

# ── New PDFs to ingest — matched by keyword to avoid filename encoding issues ─
#
# Approved book list (13 books, 14 entries including Augen Excel):
#   Bennett        — Trading Volatility (skew, term structure)
#   Benklifa       — Profiting with Iron Condor Options
#   Given          — No-Hype Options Trading
#   Jabbour/Budwick — Option Trader Handbook 2nd ed (use 2nd ed: "george m_")
#   Green          — Tax Guide for Traders
#   Harris         — Trading and Exchanges (market microstructure)
#   de Prado       — Advances in Financial ML ("lópez de prado, marcos -- 2018")
#   Augen          — Volatility Edge in Options Trading
#   Augen          — Trading Options at Expiration
#   Augen          — Excel for Stock and Option Traders (Ch 2-5)
#   Cont/Tankov    — Financial Modelling with Jump Processes (Ch 1-4, 8)
#   Bouchaud/Potters — Theory of Financial Risk (Ch 1-3, 5-6)
#   Pedersen       — Efficiently Inefficient (Ch 1-5, 9)
#   Chan           — Quantitative Trading (Ch 1-5, 7)
#
# Skipped (with reason):
#   Sinclair 2010           — superseded by 2013 (already ingested)
#   de Prado ML Asset Mgrs  — lite version of AFML (skip the second copy below)
#   Taleb Dynamic Hedging   — dealer focus, conflicts Natenberg
#   Connors/Raschke         — equity daytrading, not options
#   Arms Volume Cycles      — 1983, outdated
#   Duddella                — lighter than Bulkowski (already ingested)
#   Shefrin                 — no pipeline hook
#   Kahneman/Kiev/Richmond/Freeman/Kobayashi-Solomon — beginner/psych/value
#   Baird Option Making      — 1993, dated dealer-side
#   Overby Options Playbook  — beginner
#   Bossu/Henrotte           — Hull already covers
#   Gatheral Volatility Surface — already ingested
#   Second Leg Down          — already ingested (use the Krishnan copy already chunked)
#   Vince Mathematics        — already ingested
#
# de Prado note: two files exist; use the 2018 Wiley edition (contains full AFML text),
# skip the Cambridge "ML for Asset Managers" (lite/different book).
NEW_PDF_KEYWORDS = [
    "trading volatility - trading volatility, correlation",       # Bennett
    "profiting with iron condor",                                 # Benklifa
    "no-hype options trading",                                    # Given
    "the option trader handbook- strategies and trade -- george", # Jabbour 2nd ed
    "the tax guide for traders",                                  # Green
    "trading and exchanges- market microstructure",               # Harris
    "0645579add65f44b6809abb008da88d9",                                           # de Prado AFML (Wiley 2018, first copy)
    "_oceanofpdf.com_volatility_edge",                            # Augen Volatility Edge
    "_oceanofpdf.com_trading_options_at_expiration",              # Augen Expiration
    "_oceanofpdf.com_microsoft_excel",                            # Augen Excel
    "financial modelling with jump processes",                    # Cont/Tankov
    "theory of financial risk and derivative pricing",            # Bouchaud/Potters
    "efficiently inefficient",                                    # Pedersen
    "quantitative trading- how to build",                         # Chan
]

splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=150)


def extract_and_chunk(pdf_path: Path) -> int:
    """Extract text from PDF, chunk it, write chunk file. Returns chunk count."""
    print(f"\n📖 Extracting: {pdf_path.name[:60]}...")
    reader = PdfReader(pdf_path)
    pages_text = []
    skipped = 0
    for page in reader.pages:
        try:
            t = page.extract_text()
            if t:
                pages_text.append(t)
        except Exception:
            skipped += 1
            continue
    if skipped:
        print(f"  ⚠️  Skipped {skipped} pages with corrupt font/encoding.")

    if not pages_text:
        print("  ⚠️  No text extracted — skipping.")
        return 0

    full_text = "\n".join(pages_text)
    chunks = splitter.split_text(full_text)

    out_file = CHUNK_DIR / f"{pdf_path.stem}_chunks.txt"
    with out_file.open("w", encoding="utf-8") as f:
        for i, chunk in enumerate(chunks):
            f.write(f"[CHUNK {i}]\n{chunk}\n\n")

    print(f"  ✅ {len(pages_text)} pages → {len(chunks)} chunks → {out_file.name}")
    return len(chunks)


def rebuild_embeddings():
    """Load all chunk files, embed, save pkl."""
    print("\n🔢 Loading all chunk files...")
    all_chunks = []
    metadata   = []

    for chunk_file in sorted(CHUNK_DIR.glob("*_chunks.txt")):
        text   = chunk_file.read_text(encoding="utf-8")
        chunks = [c.strip() for c in text.split("[CHUNK") if c.strip()]
        all_chunks.extend(chunks)
        metadata.extend([chunk_file.name] * len(chunks))
        print(f"  {chunk_file.name[:60]}: {len(chunks)} chunks")

    print(f"\nTotal chunks to embed: {len(all_chunks)}")

    import os
    os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    model = SentenceTransformer("all-MiniLM-L6-v2", local_files_only=True)
    embeddings = model.encode(all_chunks, show_progress_bar=True, batch_size=64)

    with open(EMBED_PATH, "wb") as f:
        pickle.dump({"embeddings": embeddings, "chunks": all_chunks, "source": metadata}, f)

    print(f"\n✅ Embeddings saved → {EMBED_PATH}")
    print(f"   {len(all_chunks)} total chunks across {len(set(metadata))} books")


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv

    # Find matching PDFs by keyword
    new_pdf_paths = []
    for pdf in PDF_DIR.glob("*.pdf"):
        name_lower = pdf.name.lower()
        if any(kw in name_lower for kw in NEW_PDF_KEYWORDS):
            new_pdf_paths.append(pdf)

    if not new_pdf_paths:
        print(f"❌ No PDFs found matching keywords: {NEW_PDF_KEYWORDS}")
        print(f"   Searched in: {PDF_DIR}")
    else:
        print(f"Found {len(new_pdf_paths)} matching PDFs:")
        for p in new_pdf_paths:
            print(f"  • {p.name[:70]}")

    total_new_chunks = 0
    for pdf_path in new_pdf_paths:
        # Skip if chunk file already exists
        out_file = CHUNK_DIR / f"{pdf_path.stem}_chunks.txt"
        if out_file.exists():
            print(f"⏭️  Already chunked: {pdf_path.stem[:60]}")
            continue
        total_new_chunks += extract_and_chunk(pdf_path)

    if total_new_chunks > 0 or force:
        if force and total_new_chunks == 0:
            print("\n🔄 --force: Rebuilding embeddings from all existing chunk files...")
        else:
            print(f"\n📦 {total_new_chunks} new chunks added. Rebuilding embeddings...")
        rebuild_embeddings()
    else:
        print("\nℹ️  No new chunks — embeddings unchanged.")
        print("   Run with --force to rebuild anyway.")
