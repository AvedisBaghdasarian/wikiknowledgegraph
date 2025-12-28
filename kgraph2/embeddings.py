from typing import List
from .config import DEFAULT_CONFIG
from sentence_transformers import SentenceTransformer

class EmbeddingClient:
    def __init__(self, model_name: str = DEFAULT_CONFIG.embedding_model):
        self.model_name = model_name
        self.model = SentenceTransformer(self.model_name)

    def get_embedding(self, text: str) -> List[float]:
        return self.model.encode(text, show_progress_bar=False).tolist()

    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self.model.encode(texts, show_progress_bar=False).tolist()
show_progress_bar=False