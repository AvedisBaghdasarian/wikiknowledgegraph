from typing import List
from .config import DEFAULT_CONFIG
from sentence_transformers import SentenceTransformer
from .tracing import get_tracer

tracer = get_tracer(__name__)

class EmbeddingClient:
    def __init__(self, model_name: str = DEFAULT_CONFIG.embedding_model):
        self.model_name = model_name
        self.model = SentenceTransformer(self.model_name)

    @tracer.start_as_current_span("EmbeddingClient.get_embedding")
    def get_embedding(self, text: str) -> List[float]:
        return self.model.encode(text, show_progress_bar=False).tolist()

    @tracer.start_as_current_span("EmbeddingClient.get_embeddings")
    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self.model.encode(texts, show_progress_bar=False).tolist()
show_progress_bar=False