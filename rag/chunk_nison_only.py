from pathlib import Path
from langchain.text_splitter import RecursiveCharacterTextSplitter

TEXT_FILE = Path(
    "/Users/haniabadi/Documents/Github/text/core/"
    "Japanese Candlestick Charting Techniques -- Steve Nison (EPUB).txt"
)

CHUNK_DIR = Path("/Users/haniabadi/Documents/Github/options/chunks/core")
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

splitter = RecursiveCharacterTextSplitter(
    chunk_size=800,
    chunk_overlap=150
)

if __name__ == "__main__":
    text = TEXT_FILE.read_text(encoding="utf-8")
    chunks = splitter.split_text(text)

    out_file = CHUNK_DIR / "Japanese Candlestick Charting Techniques -- Steve Nison (EPUB)_chunks.txt"
    with out_file.open("w", encoding="utf-8") as f:
        for i, chunk in enumerate(chunks):
            f.write(f"[CHUNK {i}]\n")
            f.write(chunk)
            f.write("\n\n")

    print(f"Nison EPUB: {len(chunks)} chunks created")
