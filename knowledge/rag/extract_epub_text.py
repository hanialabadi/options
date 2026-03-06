from pathlib import Path
from ebooklib import epub, ITEM_DOCUMENT
from bs4 import BeautifulSoup
import sys

# ===== Entry expects EPUB path as argument =====
# Usage:
# python extract_epub_text.py path/to/book.epub

def extract_epub(epub_path: Path, out_dir: Path):
    if not epub_path.exists():
        raise FileNotFoundError(f"EPUB not found: {epub_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    out_txt = out_dir / f"{epub_path.stem}.txt"

    book = epub.read_epub(str(epub_path))
    texts = []

    for item in book.get_items():
        if item.get_type() == ITEM_DOCUMENT:
            soup = BeautifulSoup(item.get_content(), "html.parser")

            for tag in soup(["script", "style"]):
                tag.decompose()

            text = soup.get_text(separator="\n")

            if text.strip():
                texts.append(text)

    out_txt.write_text("\n\n".join(texts), encoding="utf-8")

    print(f"✅ Extracted to: {out_txt}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_epub_text.py path/to/book.epub")
        sys.exit(1)

    EPUB_PATH = Path(sys.argv[1]).expanduser()
    OUT_DIR = Path("text/core")

    extract_epub(EPUB_PATH, OUT_DIR)