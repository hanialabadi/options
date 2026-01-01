from pathlib import Path
from ebooklib import epub, ITEM_DOCUMENT
from bs4 import BeautifulSoup

# ===== Paths (absolute on purpose for stability) =====

EPUB_PATH = Path(
    "/Users/haniabadi/Documents/Github/options/core/knowloadge/"
    "Japanese Candlestick Charting Techniques - A Contemporary -- Steve Nison -- "
    "2, 2001 -- Penguin Publishing Group -- 9780735201811 -- "
    "33225b2153ccd0057ed53360891cf1bb -- Anna’s Archive.epub"
)

OUT_DIR = Path("/Users/haniabadi/Documents/Github/text/core")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_TXT = OUT_DIR / "Japanese Candlestick Charting Techniques -- Steve Nison (EPUB).txt"


# ===== EPUB extraction logic =====

def extract_epub(epub_path: Path, out_txt: Path):
    book = epub.read_epub(str(epub_path))
    texts = []

    for item in book.get_items():
        if item.get_type() == ITEM_DOCUMENT:
            soup = BeautifulSoup(item.get_content(), "html.parser")

            # Remove scripts and styles
            for tag in soup(["script", "style"]):
                tag.decompose()

            text = soup.get_text(separator="\n")

            if text.strip():
                texts.append(text)

    out_txt.write_text("\n\n".join(texts), encoding="utf-8")


# ===== Entry point =====

if __name__ == "__main__":
    extract_epub(EPUB_PATH, OUT_TXT)
    print(f"Extracted EPUB to: {OUT_TXT}")
