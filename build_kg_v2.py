import os
import logging
import argparse
from kgraph.config import DEFAULT_CONFIG
from kgraph2.models import Page, Node, Link
from kgraph2.page_parser import PageParser
from kgraph2.client import KGraphClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_file(client: KGraphClient, filepath: str):
    title = os.path.basename(filepath).rsplit('.', 1)[0]
    logging.info(f"Processing {title} from {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    page = Page(title=title, raw_content=content)
    parser = PageParser(page)
    
    nodes = []
    links = []
    
    for item in parser:
        if isinstance(item, Node):
            nodes.append(item)
        elif isinstance(item, Link):
            links.append(item)
        
        # Incremental writing if lists get too large
        if len(nodes) >= 1000:
            client.write_nodes(nodes)
            nodes = []
        if len(links) >= 1000:
            client.write_links(links)
            links = []
            
    # Final flush
    if nodes:
        client.write_nodes(nodes)
    if links:
        client.write_links(links)

def main():
    parser = argparse.ArgumentParser(description='Build knowledge graph V2 into Neo4j')
    parser.add_argument('--articles-dir', default=DEFAULT_CONFIG.articles_dir)
    parser.add_argument('--neo4j-uri', default=DEFAULT_CONFIG.neo4j_uri)
    parser.add_argument('--neo4j-user', default=DEFAULT_CONFIG.neo4j_user)
    parser.add_argument('--neo4j-password', default=DEFAULT_CONFIG.neo4j_password)
    args = parser.parse_args()

    client = KGraphClient(
        uri=args.neo4j_uri, 
        user=args.neo4j_user, 
        password=args.neo4j_password
    )
    
    try:
        client.ensure_constraints()
        
        if not os.path.exists(args.articles_dir):
            logging.error(f"Directory not found: {args.articles_dir}")
            return

        for fname in os.listdir(args.articles_dir):
            if fname.endswith('.txt'):
                process_file(client, os.path.join(args.articles_dir, fname))
                
    finally:
        client.close()

if __name__ == '__main__':
    main()
