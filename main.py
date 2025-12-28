import logging
import hashlib
from kgraph2.models import XMLMultiPageDoc, NodeType, Node, Link
from kgraph2.page_parser import PageParser
from kgraph2.client import KGraphClient
from kgraph2.embeddings import EmbeddingClient
from kgraph2.tracing import setup_tracing, get_tracer
from opentelemetry import trace

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

setup_tracing("kgraph-pipeline")
tracer = get_tracer(__name__)

def get_uid(content: str) -> str:
    """Generate a stable UID based on content."""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

def get_heading_uid(page_title: str, heading_title: str) -> str:
    """Generate a stable UID for a heading within a page."""
    return f"{page_title}#{heading_title}"

def main():
    # Hardcoded XML path as requested
    xml_path = "enwiki-20250501-pages-articles-multistream11.xml-p5399367p6899366" 
    
    # Initialize clients
    kg_client = KGraphClient()
    embed_client = EmbeddingClient()
    
    # Ensure constraints are in place
    logging.info("Ensuring Neo4j constraints...")
    kg_client.ensure_constraints()
    
    # Load XML doc
    try:
        doc = XMLMultiPageDoc(xml_path)
    except FileNotFoundError:
        logging.error(f"File not found: {xml_path}")
        return

    # Global batching for embeddings
    EMBED_BATCH_SIZE = 32
    embed_buffer = []          # Stores (node_object, content_string) tuples

    for page in doc:
        # Use start_as_current_span without a parent 'with' block to make this a root span
        with tracer.start_as_current_span("process_page") as page_span:
            try:
                page_span.set_attribute("wiki.page_title", page.title)
                logging.info(f"Processing page: {page.title}")
                
                # 1. Create Title node
                title_uid = page.title 
                title_node = Node(
                    uid=title_uid,
                    type=NodeType.TITLE,
                    properties={"title": page.title}
                )
                kg_client.write_nodes([title_node])
                
                # 2. Iterate over chunks
                parser = PageParser(page)
                for chunk in parser:
                    if chunk.type == NodeType.HEADING:
                        # Heading UID based on page title and heading title
                        # Note: chunk.content is the full heading text like "== History =="
                        # chunk.hierarchy_owner is the parent heading or page title
                        heading_text = chunk.content.strip("=").strip()
                        chunk_uid = get_heading_uid(page.title, heading_text)
                    else:
                        # Paragraph UID based on hash
                        chunk_uid = get_uid(f"{page.title}:{chunk.index}:{chunk.content[:50]}")
                    
                    # Create Node
                    chunk_node = Node(
                        uid=chunk_uid,
                        type=chunk.type,
                        properties={
                            "content": chunk.content,
                            "index": chunk.index,
                        }
                    )
                    
                    # Buffer for embeddings if it's a paragraph or heading
                    # Actually we embed both for semantic search
                    embed_buffer.append((chunk_node, chunk.content))
                    
                    # 3. Create Edges
                    
                    # Edge from hierarchy owner
                    if chunk.hierarchy_owner == page.title:
                        owner_uid = page.title
                    else:
                        # Parent is a heading
                        owner_uid = get_heading_uid(page.title, chunk.hierarchy_owner)
                    
                    kg_client.write_links([Link(source_uid=owner_uid, target_uid=chunk_uid)])
                    
                    # Extract links via get_links()
                    mentions = chunk.get_links()
                    for target_title in mentions:
                        # Link from this chunk to the mentioned Page title
                        kg_client.write_links([Link(source_uid=chunk_uid, target_uid=target_title)])
                        
                    # Batch embeddings once buffer is full
                    if len(embed_buffer) >= EMBED_BATCH_SIZE:
                        with tracer.start_as_current_span("batch_embedding_flush") as flush_span:
                            flush_span.set_attribute("batch_size", len(embed_buffer))
                            nodes = [item[0] for item in embed_buffer]
                            contents = [item[1] for item in embed_buffer]
                            
                            embeddings = embed_client.get_embeddings(contents)
                            for node, vec in zip(nodes, embeddings):
                                node.properties["embedding"] = vec
                                # Write the node to KG client ONLY AFTER embedding is attached
                                kg_client.write_nodes([node])
                            
                            embed_buffer.clear()
            except Exception as e:
                logging.error(f"Error processing page '{page.title}': {e}", exc_info=True)
                page_span.record_exception(e)
                page_span.set_status(trace.Status(trace.StatusCode.ERROR))
                continue

    # Final flush for remaining embeddings
    if embed_buffer:
        with tracer.start_as_current_span("final_embedding_flush") as flush_span:
            flush_span.set_attribute("batch_size", len(embed_buffer))
            nodes = [item[0] for item in embed_buffer]
            contents = [item[1] for item in embed_buffer]
            embeddings = embed_client.get_embeddings(contents)
            for node, vec in zip(nodes, embeddings):
                node.properties["embedding"] = vec
                kg_client.write_nodes([node])
            embed_buffer.clear()

    # Final flush to write any remaining buffered items in the client
    kg_client.close()
    logging.info("Finished processing.")

if __name__ == "__main__":
    main()
