"""Configuration defaults and helpers for kgraph ingestion."""
from dataclasses import dataclass
import os


@dataclass
class Config:
    articles_dir: str = os.getenv("KG_ARTICLES_DIR", "articles_output")
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "password")
    batch_size: int = int(os.getenv("KG_BATCH_SIZE", "1000"))
    chunk_size: int = int(os.getenv("KG_CHUNK_SIZE", "1000"))
    chunk_overlap: int = int(os.getenv("KG_CHUNK_OVERLAP", "100"))
    max_paragraph_len: int = int(os.getenv("KG_MAX_PARAGRAPH_LEN", "1000"))
    para_overlap: int = int(os.getenv("KG_PARA_OVERLAP", "50"))
    saved_batch_dir: str = os.getenv("KG_SAVED_BATCH_DIR", "saved_batches")
    # Embedding configuration
    embedding_dim: int = int(os.getenv("KG_EMBEDDING_DIM", "1536"))


DEFAULT_CONFIG = Config()
