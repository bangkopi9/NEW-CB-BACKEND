import os
import json
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

# ========================
# 📂 Path & Model Settings
# ========================
DATA_DIR = "data/faiss_index"
INDEX_PATH = os.path.join(DATA_DIR, "planville.index")
DOCS_PATH = os.path.join(DATA_DIR, "docs.json")
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# ========================
# 🧠 Load Embedding Model
# ========================
model = SentenceTransformer(MODEL_NAME)


# ========================
# 🔧 Build Vector Store
# ========================
def build_vector_store():
    if not os.path.exists(DOCS_PATH):
        raise FileNotFoundError(f"❌ File {DOCS_PATH} tidak ditemukan. Jalankan scraping dulu.")

    with open(DOCS_PATH, encoding="utf-8") as f:
        docs = json.load(f)

    embeddings = model.encode(docs, show_progress_bar=True)
    index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(np.array(embeddings))

    os.makedirs(DATA_DIR, exist_ok=True)
    faiss.write_index(index, INDEX_PATH)
    with open(DOCS_PATH, "w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)

    print(f"✅ Index saved to {INDEX_PATH} | Total Docs: {len(docs)}")


# ========================
# 🔍 Query Vector Index
# ========================
def query_index(query, top_k=3):
    if not os.path.exists(INDEX_PATH) or not os.path.exists(DOCS_PATH):
        raise FileNotFoundError("❌ Index atau dokumen belum dibangun. Jalankan `build_vector_store()` dulu.")

    index = faiss.read_index(INDEX_PATH)
    with open(DOCS_PATH, encoding="utf-8") as f:
        docs = json.load(f)

    q_emb = model.encode([query])
    D, I = index.search(np.array(q_emb), top_k)
    return [docs[i] for i in I[0]]


# ========================
# 🚀 CLI Mode
# ========================
if __name__ == "__main__":
    print("🔁 Building vector store from docs.json...")
    build_vector_store()
