import os
from dataclasses import dataclass

@dataclass
class Config:
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "password")
    batch_size: int = int(os.getenv("BATCH_SIZE", "5000"))
    embedding_model: str = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "4"))

DEFAULT_CONFIG = Config()
