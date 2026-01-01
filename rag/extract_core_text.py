from pathlib import Path
from pypdf import PdfReader

CORE_PDF_DIR = Path("/Users/haniabadi/Documents/Github/options/core/knowloadge")
CORE_TEXT_DIR = Path("../text/core")
CORE_TEXT_DIR.mkdir(parents=True, exist_ok=True)

def extract_pdf(pdf_path: Path, out_path: Path):
    reader = PdfReader(pdf_path)
    text = []
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            text.append(page_text)
    out_path.write_text("\n".join(text), encoding="utf-8")

if __name__ == "__main__":
    for pdf in CORE_PDF_DIR.glob("*.pdf"):
        out = CORE_TEXT_DIR / f"{pdf.stem}.txt"
        print(f"Extracting: {pdf.name}")
        extract_pdf(pdf, out)
