from neo4j import GraphDatabase
import logging
import time
from typing import List, Union, Dict, Any
from .models import Node, Link, NodeType
from .config import DEFAULT_CONFIG
from .tracing import get_tracer

tracer = get_tracer(__name__)

class KGraphClient:
    def __init__(self, uri: str = DEFAULT_CONFIG.neo4j_uri, 
                 user: str = DEFAULT_CONFIG.neo4j_user, 
                 password: str = DEFAULT_CONFIG.neo4j_password, 
                 batch_size: int = DEFAULT_CONFIG.batch_size):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size
        self._node_buffer: List[Node] = []
        self._link_buffer: List[Link] = []

    @tracer.start_as_current_span("KGraphClient.close")
    def close(self):
        self.flush()
        self._driver.close()

    @tracer.start_as_current_span("KGraphClient.ensure_constraints")
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

    @tracer.start_as_current_span("KGraphClient.flush")
    def flush(self):
        """Manually flush all buffered nodes and links to Neo4j."""
        self.flush_nodes()
        self.flush_links()

    def write_nodes(self, nodes: List[Node]):
        if not nodes: return
        self._node_buffer.extend(nodes)
        if len(self._node_buffer) >= self.batch_size:
            self.flush_nodes()

    @tracer.start_as_current_span("KGraphClient.flush_nodes")
    def flush_nodes(self):
        if not self._node_buffer: return        
        # Group by type for efficiency
        by_type = {t: [] for t in NodeType}
        for n in self._node_buffer:
            by_type[n.type].append({**n.properties, "uid": n.uid})

        for node_type, batch in by_type.items():
            if not batch: continue
            label = node_type.value
            # We MERGE on the generic 'Resource' label to avoid label-mismatch conflicts.
            # Then we SET the specific label and properties.
            cypher = f"""
            UNWIND $batch AS row
            MERGE (n:Resource {{uid: row.uid}})
            SET n:{label}, n += row
            """
            self._run_batch(cypher, batch, label=f"Nodes:{label}")
        
        self._node_buffer = []

    def write_links(self, links: List[Link]):
        if not links: return
        self._link_buffer.extend(links)
        if len(self._link_buffer) >= self.batch_size:
            self.flush_links()

    @tracer.start_as_current_span("KGraphClient.flush_links")
    def flush_links(self):
        if not self._link_buffer: return
        
        # Every link is (a)-[:LINK]->(b)
        # Using a MERGE on the generic 'Resource' label for both source and target.
        # This ensures the link is created even if nodes are flushed out of order or as stubs.
        # Default stubs get the 'Title' label.
        
        batch_data = [{"source": l.source_uid, "target": l.target_uid} for l in self._link_buffer]
        
        cypher = """
        UNWIND $batch AS row
        MERGE (a:Resource {uid: row.source})
        MERGE (b:Resource {uid: row.target})
        ON CREATE SET b:Title
        MERGE (a)-[:LINK]->(b)
        """
        self._run_batch(cypher, batch_data, label="Links")
        self._link_buffer = []

    def _run_batch(self, cypher: str, batch: List[Dict[str, Any]], label: str):
        with tracer.start_as_current_span("neo4j_run_batch") as span:
            span.set_attribute("neo4j.batch_label", label)
            span.set_attribute("neo4j.batch_size", len(batch))
            for i in range(0, len(batch), self.batch_size):
                sub_batch = batch[i:i+self.batch_size]
                start = time.perf_counter()
                with tracer.start_as_current_span("neo4j_session_execute") as sub_span:
                    sub_span.set_attribute("neo4j.sub_batch_size", len(sub_batch))
                    with self._driver.session() as session:
                        try:
                            session.execute_write(lambda tx: tx.run(cypher, batch=sub_batch))
                        except Exception as e:
                            logging.error(f"Failed to execute batch for {label}. Cypher: {cypher}")
                            logging.error(f"Batch sample (first 2): {sub_batch[:2]}")
                            raise e
                logging.info(f"Wrote {label} batch {i}-{i+len(sub_batch)} in {(time.perf_counter()-start)*1000:.1f} ms")
