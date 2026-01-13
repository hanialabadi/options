from pathlib import Path
from langchain.text_splitter import RecursiveCharacterTextSplitter

TEXT_DIR = Path("/Users/haniabadi/Documents/Github/text/core")
CHUNK_DIR = Path("/Users/haniabadi/Documents/Github/options/chunks/core")
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

splitter = RecursiveCharacterTextSplitter(
    chunk_size=800,
    chunk_overlap=150
)

if __name__ == "__main__":
    for txt in TEXT_DIR.glob("*.txt"):
        text = txt.read_text(encoding="utf-8")
        chunks = splitter.split_text(text)

        out_file = CHUNK_DIR / f"{txt.stem}_chunks.txt"
        with out_file.open("w", encoding="utf-8") as f:
            for i, chunk in enumerate(chunks):
                f.write(f"[CHUNK {i}]\n")
                f.write(chunk)
                f.write("\n\n")

        print(f"{txt.name}: {len(chunks)} chunks created")
