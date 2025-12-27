import logging
import hashlib
from kgraph2.models import XMLMultiPageDoc, NodeType, Node, Link, Page
from kgraph2.page_parser import PageParser
from kgraph2.client import KGraphClient
from kgraph.embeddings import EmbeddingClient

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

    for page in doc:
        logging.info(f"Processing page: {page.title}")
        
        nodes_to_write = []
        links_to_write = []
        
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
            
            # Generate embedding
            embedding = embed_client.get_embedding(chunk.content)
            
            # Create Node
            chunk_node = Node(
                uid=chunk_uid,
                type=chunk.type,
                properties={
                    "content": chunk.content,
                    "index": chunk.index,
                    "embedding": embedding
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
            # NOTE: PageParserInner uses hierarchy_stack which contains titles.
            # To link to the correct Heading node, we'd need to track Heading UIDs by their titles.
            # For this 'intentionally simple' version, we'll link to owner_uid and assume 
            # we can find it in Neo4j.
            
            links_to_write.append(Link(source_uid=owner_uid, target_uid=chunk_uid))
            
            # Extract links via get_links()
            mentions = chunk.get_links()
            for target_title in mentions:
                # Link from this chunk to the mentioned Page title
                links_to_write.append(Link(source_uid=chunk_uid, target_uid=target_title))
                
        # Write to Neo4j
        kg_client.write_nodes(nodes_to_write)
        kg_client.write_links(links_to_write)

    kg_client.close()
    logging.info("Finished processing.")

if __name__ == "__main__":
    main()
