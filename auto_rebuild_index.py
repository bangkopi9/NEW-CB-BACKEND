import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import faiss
from sentence_transformers import SentenceTransformer

# ============
# 🔧 Konfigurasi
# ============
BASE_URL = "https://planville.de"
PAGES = ["", "/leistungen", "/kontakt", "/foerderung", "/ueber-uns", "/hausbesitzer", "/hausbesitzer/photovoltaikanlage"]
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  # Bisa diganti model lain jika perlu
DOC_PATH = "docs.json"
INDEX_PATH = "planville.index"

# ====================
# 🧹 Step 1: Scraping
# ====================
def scrape_website():
    docs = []
    for page in PAGES:
        url = urljoin(BASE_URL, page)
        try:
            print(f"📥 Scraping: {url}")
            res = requests.get(url, timeout=10)
            soup = BeautifulSoup(res.text, "html.parser")
            tags = soup.find_all(["p", "h1", "h2", "li"])

            for tag in tags:
                text = tag.get_text(strip=True)
                if len(text) >= 30:
                    docs.append(text)

        except Exception as e:
            print(f"[ERROR] {url} → {e}")

    print(f"✅ Total scraped chunks: {len(docs)}")
    with open(DOC_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

    return docs

# ====================
# 🧠 Step 2: Rebuild Index
# ====================
def rebuild_index(docs):
    print("🔍 Rebuilding FAISS index...")
    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(docs, show_progress_bar=True)

    index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(embeddings)

    faiss.write_index(index, INDEX_PATH)
    print(f"✅ Saved new index to: {INDEX_PATH}")

# ====================
# 🚀 RUN SCRIPT
# ====================
if __name__ == "__main__":
    print("⚙️ Starting auto rebuild...")
    documents = scrape_website()
    rebuild_index(documents)
    print("🎉 Rebuild complete.")