import requests
from bs4 import BeautifulSoup
import re

def get_scraped_context(query, max_results=5):
    """
    Scrape konten dari halaman website Planville secara langsung,
    mencari elemen teks yang relevan dengan query pengguna.
    """
    urls = [
        "https://planville.de/hausbesitzer/photovoltaikanlage/",
        "https://planville.de/hausbesitzer/",
        "https://planville.de/",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PlanvilleBot/1.0; +https://planville.de)"
    }

    results = []
    query_words = re.findall(r'\w+', query.lower())

    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')

            # Ambil teks dari tag yang bernilai informasi
            tags = soup.find_all(['p', 'h2', 'h3', 'li'])

            for tag in tags:
                text = tag.get_text().strip()
                if len(text) < 30:
                    continue

                # Relevansi: jumlah kata query yang cocok
                score = sum(1 for word in query_words if word in text.lower())

                if score > 0:
                    results.append(text)

                if len(results) >= max_results:
                    return results

        except requests.exceptions.RequestException as e:
            print(f"[❌ Scraping Error] {url} → {e}")
            continue

    return results or ["Maaf, tidak ada konteks relevan ditemukan dari website."]

# 🧪 Contoh test
# print(get_scraped_context("photovoltaik anlage dach"))
