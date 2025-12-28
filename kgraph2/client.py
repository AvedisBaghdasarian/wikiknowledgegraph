from neo4j import GraphDatabase
import logging
import time
from typing import List, Union, Dict, Any
from .models import Node, Link, NodeType
from .config import DEFAULT_CONFIG

class KGraphClient:
    def __init__(self, uri: str = DEFAULT_CONFIG.neo4j_uri, 
                 user: str = DEFAULT_CONFIG.neo4j_user, 
                 password: str = DEFAULT_CONFIG.neo4j_password, 
                 batch_size: int = DEFAULT_CONFIG.batch_size):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size
        self._node_buffer: List[Node] = []
        self._link_buffer: List[Link] = []

    def close(self):
        self.flush()
        self._driver.close()

    def ensure_constraints(self):
        with self._driver.session() as session:
            # We use uid as the primary identifier across all node types
            # Added 'Resource' as a generic label for all nodes to improve query performance
            for label in ["Title", "Heading", "Paragraph", "Resource"]:
                try:
                    session.execute_write(lambda tx: tx.run(
                        f"CREATE CONSTRAINT {label.lower()}_uid IF NOT EXISTS FOR (n:{label}) REQUIRE n.uid IS UNIQUE"
                    ))
                except Exception as e:
                    logging.warning(f"Could not ensure constraint for {label}: {e}")

    def flush(self):
        """Manually flush all buffered nodes and links to Neo4j."""
        self.flush_nodes()
        self.flush_links()

    def write_nodes(self, nodes: List[Node]):
        if not nodes: return
        self._node_buffer.extend(nodes)
        if len(self._node_buffer) >= self.batch_size:
            self.flush_nodes()

    def flush_nodes(self):
        if not self._node_buffer: return
        
        # Group by type for efficiency if needed, but for simplicity we can use UNWIND with dynamic labels
        # Actually, Neo4j doesn't support dynamic labels in MERGE easily without APOC or multiple queries.
        # Since we only have 3 types, we'll split them.
        by_type = {t: [] for t in NodeType}
        for n in self._node_buffer:
            by_type[n.type].append({**n.properties, "uid": n.uid})

        for node_type, batch in by_type.items():
            if not batch: continue
            label = node_type.value
            # We add the 'Resource' label to every node for efficient cross-type lookups
            cypher = f"""
            UNWIND $batch AS row
            MERGE (n:{label} {{uid: row.uid}})
            SET n:Resource, n += row
            """
            self._run_batch(cypher, batch, label=f"Nodes:{label}")
        
        self._node_buffer = []

    def write_links(self, links: List[Link]):
        if not links: return
        self._link_buffer.extend(links)
        if len(self._link_buffer) >= self.batch_size:
            self.flush_links()

    def flush_links(self):
        if not self._link_buffer: return
        
        # Every link is (a)-[:LINK]->(b)
        # Using a MATCH on the generic 'Resource' label which has a unique constraint on 'uid'.
        # We use CREATE instead of MERGE for the relationship to avoid the expensive existence check,
        # since we are doing a bulk load and don't expect duplicate relationships in the same batch.
        # If duplicates are possible across batches, MERGE is safer but slower.
        # For 'even faster' ingestion, CREATE is preferred.
        
        batch_data = [{"source": l.source_uid, "target": l.target_uid} for l in self._link_buffer]
        
        cypher = """
        UNWIND $batch AS row
        MATCH (a:Resource {uid: row.source})
        MATCH (b:Resource {uid: row.target})
        CREATE (a)-[:LINK]->(b)
        """
        self._run_batch(cypher, batch_data, label="Links")
        self._link_buffer = []

    def _run_batch(self, cypher: str, batch: List[Dict[str, Any]], label: str):
        for i in range(0, len(batch), self.batch_size):
            sub_batch = batch[i:i+self.batch_size]
            start = time.perf_counter()
            with self._driver.session() as session:
                session.execute_write(lambda tx: tx.run(cypher, batch=sub_batch))
            logging.info(f"Wrote {label} batch {i}-{i+len(sub_batch)} in {(time.perf_counter()-start)*1000:.1f} ms")
