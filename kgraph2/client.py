from neo4j import GraphDatabase
import logging
import time
from typing import List, Union, Dict, Any
from .models import Node, Link, NodeType
from kgraph.config import DEFAULT_CONFIG

class KGraphClient:
    def __init__(self, uri: str = DEFAULT_CONFIG.neo4j_uri, 
                 user: str = DEFAULT_CONFIG.neo4j_user, 
                 password: str = DEFAULT_CONFIG.neo4j_password, 
                 batch_size: int = DEFAULT_CONFIG.batch_size):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size

    def close(self):
        self._driver.close()

    def ensure_constraints(self):
        with self._driver.session() as session:
            # We use uid as the primary identifier across all node types
            for label in ["Title", "Heading", "Paragraph"]:
                try:
                    session.execute_write(lambda tx: tx.run(
                        f"CREATE CONSTRAINT {label.lower()}_uid IF NOT EXISTS FOR (n:{label}) REQUIRE n.uid IS UNIQUE"
                    ))
                except Exception as e:
                    logging.warning(f"Could not ensure constraint for {label}: {e}")

    def write_nodes(self, nodes: List[Node]):
        if not nodes: return
        
        # Group by type for efficiency if needed, but for simplicity we can use UNWIND with dynamic labels
        # Actually, Neo4j doesn't support dynamic labels in MERGE easily without APOC or multiple queries.
        # Since we only have 3 types, we'll split them.
        by_type = {t: [] for t in NodeType}
        for n in nodes:
            by_type[n.type].append({**n.properties, "uid": n.uid})

        for node_type, batch in by_type.items():
            if not batch: continue
            label = node_type.value
            cypher = f"""
            UNWIND $batch AS row
            MERGE (n:{label} {{uid: row.uid}})
            SET n += row
            """
            self._run_batch(cypher, batch, label=f"Nodes:{label}")

    def write_links(self, links: List[Link]):
        if not links: return
        
        # Every link is (a)-[:LINK]->(b)
        # However, we don't know the labels of source/target just from the Link object in this refactor
        # unless we store them or search all possible labels.
        # To keep it performant, we can use a generic Node label if we add it to all nodes, 
        # or just MATCH by uid which is unique across the DB.
        
        batch_data = [{"source": l.source_uid, "target": l.target_uid} for l in links]
        
        # Using a MATCH on a generic 'uid' across all node labels. 
        # For this to be fast, we need a 'Resource' or 'Node' base label, or just rely on the constraints.
        # Since we didn't add a base label, we'll MATCH without labels but with UID.
        cypher = """
        UNWIND $batch AS row
        MATCH (a {uid: row.source})
        MATCH (b {uid: row.target})
        MERGE (a)-[:LINK]->(b)
        """
        self._run_batch(cypher, batch_data, label="Links")

    def _run_batch(self, cypher: str, batch: List[Dict[str, Any]], label: str):
        for i in range(0, len(batch), self.batch_size):
            sub_batch = batch[i:i+self.batch_size]
            start = time.perf_counter()
            with self._driver.session() as session:
                session.execute_write(lambda tx: tx.run(cypher, batch=sub_batch))
            logging.info(f"Wrote {label} batch {i}-{i+len(sub_batch)} in {(time.perf_counter()-start)*1000:.1f} ms")
