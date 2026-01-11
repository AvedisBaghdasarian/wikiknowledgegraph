from neo4j import GraphDatabase
import logging
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union, Dict, Any
from .models import Node, Link, NodeType
from .config import DEFAULT_CONFIG
from .tracing import get_tracer

tracer = get_tracer(__name__)

class KGraphClient:
    def __init__(self, uri: str = DEFAULT_CONFIG.neo4j_uri, 
                 user: str = DEFAULT_CONFIG.neo4j_user, 
                 password: str = DEFAULT_CONFIG.neo4j_password, 
                 batch_size: int = DEFAULT_CONFIG.batch_size,
                 max_concurrency: int = DEFAULT_CONFIG.max_concurrency):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = batch_size
        self._node_buffer: List[Node] = []
        self._link_buffer: List[Link] = []
        self._buffer_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=max_concurrency)
        self._semaphore = threading.Semaphore(max_concurrency)
        self._futures = []

    @tracer.start_as_current_span("KGraphClient.close")
    def close(self):
        self.flush()
        # Wait for all background tasks to complete
        logging.info("Waiting for background Neo4j tasks to complete...")
        self._executor.shutdown(wait=True)
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
        with self._buffer_lock:
            self._flush_nodes_unlocked()
            self._flush_links_unlocked()

    def write_nodes(self, nodes: List[Node]):
        if not nodes: return
        with self._buffer_lock:
            logging.debug(f"Buffering {len(nodes)} nodes. Current buffer size: {len(self._node_buffer)}")
            self._node_buffer.extend(nodes)
            if len(self._node_buffer) >= self.batch_size:
                logging.debug(f"Node buffer reached batch size ({self.batch_size}). Flushing...")
                self._flush_nodes_unlocked()

    @tracer.start_as_current_span("KGraphClient.flush_nodes")
    def flush_nodes(self):
        with self._buffer_lock:
            self._flush_nodes_unlocked()

    def _flush_nodes_unlocked(self):
        """Internal method to flush nodes. Assumes _buffer_lock is held."""
        if not self._node_buffer: return        
        logging.debug(f"Flushing {len(self._node_buffer)} nodes from buffer.")
        nodes_to_flush = self._node_buffer
        self._node_buffer = []

        # Group by type for efficiency
        by_type = {t: [] for t in NodeType}
        for n in nodes_to_flush:
            by_type[n.type].append({**n.properties, "uid": n.uid})

        for node_type, batch in by_type.items():
            if not batch: continue
            label = node_type.value
            logging.debug(f"Submitting {len(batch)} nodes of type {label} to executor.")
            # Optimization: Using a single MERGE with SET is standard, but we ensure
            # that we're matching on the indexed UID. 
            # Given "mostly new" nodes, we still use MERGE for safety.
            # Optimization: Smart MERGE - avoid n += row on match if unnecessary
            # Using row properties directly in ON MATCH to be explicit.
            props_to_set = ", ".join([f"n.{k} = row.{k}" for k in batch[0].keys() if k != "uid"])
            
            cypher = f"""
            UNWIND $batch AS row
            MERGE (n:Resource {{uid: row.uid}})
            ON CREATE SET n:{label}, n += row
            ON MATCH SET {props_to_set}
            """
            self._run_batch(cypher, batch, label=f"Nodes:{label}")

    def write_links(self, links: List[Link]):
        if not links: return
        with self._buffer_lock:
            logging.debug(f"Buffering {len(links)} links. Current buffer size: {len(self._link_buffer)}")
            self._link_buffer.extend(links)
            if len(self._link_buffer) >= self.batch_size:
                logging.debug(f"Link buffer reached batch size ({self.batch_size}). Flushing...")
                self._flush_links_unlocked()

    @tracer.start_as_current_span("KGraphClient.flush_links")
    def flush_links(self):
        with self._buffer_lock:
            self._flush_links_unlocked()

    def _flush_links_unlocked(self):
        """Internal method to flush links. Assumes _buffer_lock is held."""
        if not self._link_buffer: return
        logging.debug(f"Flushing {len(self._link_buffer)} links from buffer.")
        links_to_flush = self._link_buffer
        self._link_buffer = []
        
        # Optimization: Use MATCH where possible, and only MERGE for stubs.
        # This significantly reduces the overhead of re-creating/checking existing nodes.
        batch_data = [{"source": l.source_uid, "target": l.target_uid} for l in links_to_flush]
        logging.debug(f"Submitting {len(batch_data)} links to executor.")

        # Optimization: Match-Match-Merge pattern. 
        # Don't MERGE target nodes during link creation. 
        # This assumes nodes are already created or will be created in their own flush.
        cypher = """
        UNWIND $batch AS row
        MATCH (a:Resource {uid: row.source})
        MATCH (b:Resource {uid: row.target})
        MERGE (a)-[:LINK]->(b)
        """
        self._run_batch(cypher, batch_data, label="Links")

    def _run_batch(self, cypher: str, batch: List[Dict[str, Any]], label: str):
        # We split the batch into sub-batches and submit each as a background task
        num_sub_batches = 0
        for i in range(0, len(batch), self.batch_size):
            sub_batch = batch[i:i+self.batch_size]
            logging.debug(f"Submitting sub-batch {i} for {label} (size: {len(sub_batch)})")
            self._executor.submit(self._execute_with_semaphore, cypher, sub_batch, label, i)
            num_sub_batches += 1
        logging.debug(f"Submitted {num_sub_batches} total sub-batches for {label}.")

    def _execute_with_semaphore(self, cypher: str, sub_batch: List[Dict[str, Any]], label: str, index_offset: int):
        logging.debug(f"Background task waiting for semaphore: {label} offset {index_offset}")
        with self._semaphore:
            logging.debug(f"Background task acquired semaphore: {label} offset {index_offset}")
            with tracer.start_as_current_span("neo4j_run_batch") as span:
                span.set_attribute("neo4j.batch_label", label)
                span.set_attribute("neo4j.batch_size", len(sub_batch))
                
                # Calculate size via JSON serialization
                batch_json = json.dumps(sub_batch)
                batch_size_bytes = len(batch_json.encode('utf-8'))
                
                start = time.perf_counter()
                with tracer.start_as_current_span("neo4j_session_execute") as sub_span:
                    sub_span.set_attribute("neo4j.sub_batch_size", len(sub_batch))
                    sub_span.set_attribute("neo4j.batch_size_bytes", batch_size_bytes)
                    
                    try:
                        with self._driver.session() as session:
                            session.execute_write(lambda tx: tx.run(cypher, batch=sub_batch))
                    except Exception as e:
                        logging.error(f"Failed to execute batch for {label}. Cypher: {cypher}")
                        logging.error(f"Batch sample (first 2): {sub_batch[:2]}")
                        # We don't want to swallow exceptions in background threads without at least logging
                        # and potentially crashing if they are critical.
                        raise e
                
                duration = time.perf_counter() - start
                latency_ms = duration * 1000
                throughput_mb_s = (batch_size_bytes / (1024 * 1024)) / duration if duration > 0 else 0
                
                logging.info(
                    f"Wrote {label} batch {index_offset}-{index_offset+len(sub_batch)} | "
                    f"Size: {batch_size_bytes/1024:.2f} KB | "
                    f"Latency: {latency_ms:.1f} ms | "
                    f"Throughput: {throughput_mb_s:.2f} MB/s"
                )
