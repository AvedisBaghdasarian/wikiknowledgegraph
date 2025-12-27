"""Neo4j client wrapper with batch writing helpers."""
from neo4j import GraphDatabase
import logging
import time
import os
import json
from typing import List
from .config import DEFAULT_CONFIG


class Neo4jClient:
    def __init__(self, uri: str = DEFAULT_CONFIG.neo4j_uri, user: str = DEFAULT_CONFIG.neo4j_user, password: str = DEFAULT_CONFIG.neo4j_password, batch_size: int = DEFAULT_CONFIG.batch_size, saved_batch_dir: str = DEFAULT_CONFIG.saved_batch_dir, embedder=None, embed_dim: int = None):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size
        self.saved_batch_dir = saved_batch_dir
        self.embedder = embedder
        self.embed_dim = embed_dim or DEFAULT_CONFIG.embedding_dim

    def close(self):
        self._driver.close()

    def ensure_constraints(self):
        with self._driver.session() as session:
            try:
                session.execute_write(lambda tx: tx.run("CREATE CONSTRAINT paragraph_id IF NOT EXISTS FOR (p:Paragraph) REQUIRE p.id IS UNIQUE"))
                logging.info("Ensured :Paragraph(id) uniqueness constraint")
            except Exception as e:
                logging.warning(f"Could not ensure constraint: {e}")
            # Try to create a vector index for paragraph embeddings (Neo4j v5+)
            try:
                # Best-effort: not all Neo4j editions support vector indexes; ignore failures
                idx_cypher = (
                    f"CREATE VECTOR INDEX paragraph_embedding_vector_index IF NOT EXISTS FOR (p:Paragraph) ON (p.embedding) OPTIONS {{indexProvider: 'vector-1', dimension: {self.embed_dim}, similarityFunction: 'cosine'}}"
                )
                session.execute_write(lambda tx: tx.run(idx_cypher))
                logging.info("Ensured paragraph embedding vector index (if supported)")
            except Exception as e:
                logging.debug(f"Vector index creation skipped/not supported: {e}")

    def write_title(self, title: str):
        with self._driver.session() as session:
            start = time.perf_counter()
            session.execute_write(lambda tx: tx.run("MERGE (t:Title {name: $title})", title=title))
            logging.info(f"Wrote Title {title} in {(time.perf_counter()-start)*1000:.1f} ms")

    def run_cypher_batches(self, cypher: str, data: List[dict], label: str = None):
        os.makedirs(self.saved_batch_dir, exist_ok=True)
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i+self.batch_size]
            if label and len(batch) == 661:
                try:
                    fname = f"{label}_{i}_{i+len(batch)}_661.json"
                    fpath = os.path.join(self.saved_batch_dir, fname)
                    with open(fpath, 'w', encoding='utf-8') as fh:
                        json.dump({'label': label, 'start': i, 'end': i+len(batch), 'batch': batch}, fh, ensure_ascii=False, indent=2)
                    logging.info(f"Saved 661-op batch to {fpath}")
                except Exception as e:
                    logging.debug(f"Failed to save batch: {e}")
            try:
                payload_bytes = len(json.dumps(batch, ensure_ascii=False).encode('utf-8'))
            except Exception:
                payload_bytes = sum(len(repr(r)) for r in batch)
            logging.info(f"Batch {label or ''} {i}-{i+len(batch)} payload ~{payload_bytes/1024:.2f} KB")
            start = time.perf_counter()
            with self._driver.session() as session:
                session.execute_write(lambda tx: tx.run(cypher, batch=batch))
            logging.info(f"Wrote batch {label or ''} {i}-{i+len(batch)} in {(time.perf_counter()-start)*1000:.1f} ms")

    def write_paragraph_embeddings(self, paragraphs: List[dict]):
        """Compute embeddings for paragraph dicts and write them to Neo4j as `p.embedding`.

        Expects each paragraph dict to have an `id` and `text` field.
        """
        if not self.embedder:
            logging.info("No embedder provided; skipping embedding write")
            return

        # prepare batches of {id, embedding}
        rows = []
        for p in paragraphs:
            pid = p.get('id')
            text = p.get('text', '')
            if pid is None:
                continue
            try:
                emb = self.embedder.get_embedding(text)
            except Exception as e:
                logging.debug(f"Embedding generation failed for {pid}: {e}")
                continue
            rows.append({'id': pid, 'embedding': emb})

        cypher = """
        UNWIND $batch AS row
        MATCH (p:Paragraph {id: row.id})
        SET p.embedding = row.embedding
        """
        # write in batches
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i:i+self.batch_size]
            try:
                start = time.perf_counter()
                with self._driver.session() as session:
                    session.execute_write(lambda tx: tx.run(cypher, batch=batch))
                logging.info(f"Wrote paragraph embeddings {i}-{i+len(batch)} in {(time.perf_counter()-start)*1000:.1f} ms")
            except Exception as e:
                logging.warning(f"Failed to write paragraph embeddings batch {i}-{i+len(batch)}: {e}")
