import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import os
from datetime import datetime

# 🌍 Konfigurasi dasar
BASE_URL = "https://planville.de"
PAGES = [
    "", "/leistungen", "/kontakt", "/foerderung", "/ueber-uns",
    "/hausbesitzer", "/hausbesitzer/photovoltaikanlage"
]
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "docs.json")
MIN_WORDS = 6  # teks minimal agar dianggap layak

def scrape_planville():
    docs = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PlanvilleScraper/1.0; +https://planville.de)"
    }

    for page in PAGES:
        url = urljoin(BASE_URL, page)
        try:
            print(f"📥 Scraping: {url}")
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            elements = soup.find_all(['p', 'h1', 'h2', 'h3', 'li'])

            for el in elements:
                text = el.get_text(strip=True)
                if text and len(text.split()) >= MIN_WORDS:
                    docs.append(text)

        except requests.exceptions.RequestException as e:
            print(f"[❌ ERROR] {url} → {e}")

    # Simpan hasil scraping
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

    print(f"✅ Selesai: {len(docs)} konten disimpan ke {OUTPUT_FILE}")
    print(f"🕒 Tanggal scrape: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    scrape_planville()
