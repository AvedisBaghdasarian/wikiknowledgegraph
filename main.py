import logging
import hashlib
from kgraph2.models import XMLMultiPageDoc, NodeType, Node, Link
from kgraph2.page_parser import PageParser
from kgraph2.client import KGraphClient
from kgraph2.embeddings import EmbeddingClient

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_uid(content: str) -> str:
    """Generate a stable UID based on content."""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

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

    # Global batching across pages
    BATCH_SIZE = 32
    embed_buffer = []          # Stores chunk contents for embedding
    nodes_to_write = []        # Stores Node objects globally
    links_to_write = []        # Stores Link objects globally

    for page in doc:
        logging.info(f"Processing page: {page.title}")
        
        # 1. Create Title node
        title_uid = page.title # Title can be its own UID if we assume uniqueness across pages
        title_node = Node(
            uid=title_uid,
            type=NodeType.TITLE,
            properties={"title": page.title}
        )
        nodes_to_write.append(title_node)
        
        # 2. Iterate over chunks
        parser = PageParser(page)
        for chunk in parser:
            chunk_uid = get_uid(f"{page.title}:{chunk.index}:{chunk.content[:50]}")
            
            # Add chunk content to buffer for batch embedding
            embed_buffer.append(chunk.content)
            
            # Create Node without embedding yet
            chunk_node = Node(
                uid=chunk_uid,
                type=chunk.type,
                properties={
                    "content": chunk.content,
                    "index": chunk.index,
                    "embedding": None  # Will fill after batch embedding
                }
            )
            nodes_to_write.append(chunk_node)
            
            # 3. Create Edges
            
            # Edge from hierarchy owner (Title or Heading)
            # If hierarchy_owner is the page title, link to title node
            # Note: hierarchy_owner is currently a string (title), 
            # we need to be careful with UID matching for headings if we want to link to them.
            # For simplicity, if hierarchy_owner is page.title, we link to title_uid.
            # If it's something else, it's a heading.
            owner_uid = chunk.hierarchy_owner if chunk.hierarchy_owner == page.title else chunk.hierarchy_owner
            links_to_write.append(Link(source_uid=owner_uid, target_uid=chunk_uid))
            
            # Extract links via get_links()
            mentions = chunk.get_links()
            for target_title in mentions:
                # Link from this chunk to the mentioned Page title
                links_to_write.append(Link(source_uid=chunk_uid, target_uid=target_title))
                
            # Batch embeddings once buffer is full
            if len(embed_buffer) >= BATCH_SIZE:
                embeddings = embed_client.get_embeddings(embed_buffer)
                for node, vec in zip(nodes_to_write[-BATCH_SIZE:], embeddings):
                    node.properties["embedding"] = vec
                embed_buffer.clear()

    # Final flush for remaining embeddings
    if embed_buffer:
        embeddings = embed_client.get_embeddings(embed_buffer)
        for node, vec in zip(nodes_to_write[-len(embed_buffer):], embeddings):
            node.properties["embedding"] = vec
        embed_buffer.clear()

    # Write all nodes and links to Neo4j at once
    kg_client.write_nodes(nodes_to_write)
    kg_client.write_links(links_to_write)

    # Final flush to write any remaining buffered items
    kg_client.flush()
    kg_client.close()
    logging.info("Finished processing.")

if __name__ == "__main__":
    main()