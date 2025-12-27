import os
import logging
import argparse
from kgraph.config import DEFAULT_CONFIG
from kgraph.parser import parse_article
from kgraph.neo4j_client import Neo4jClient
from kgraph.embeddings import EmbeddingClient


logging.basicConfig(level=logging.INFO, format='%(message)s')


def write_article_to_neo4j(client: Neo4jClient, article_data: dict):
    title = article_data['title']
    logging.info(f"Writing Title node for: {title}")
    client.write_title(title)

    if article_data['headings']:
        logging.info(f"Writing {len(article_data['headings'])} heading nodes/edges for: {title}")
        cypher = """
        UNWIND $batch AS row
        MERGE (h:Heading {title: row.title, heading: row.heading, line: row.line})
        FOREACH (_ IN CASE WHEN row.parent = row.title THEN [1] ELSE [] END |
            MERGE (p:Title {name: row.parent})
            MERGE (h)-[:LINK]->(p)
        )
        FOREACH (_ IN CASE WHEN row.parent <> row.title THEN [1] ELSE [] END |
            MERGE (p:Heading {title: row.title, heading: row.parent})
            MERGE (h)-[:LINK]->(p)
        )
        """
        client.run_cypher_batches(cypher, article_data['headings'], label='headings')

    if article_data['paragraphs']:
        logging.info(f"Writing {len(article_data['paragraphs'])} paragraph nodes for: {title}")
        cypher = """
        UNWIND $batch AS row
        MERGE (p:Paragraph {id: row.id})
        SET p.text = row.text, p.title = row.title, p.line = row.line, p.chunk_idx = row.chunk_idx, p.parent_heading = row.parent_heading
        """
        client.run_cypher_batches(cypher, article_data['paragraphs'], label='paragraphs')

    if article_data['hierarchy']:
        logging.info(f"Writing {len(article_data['hierarchy'])} hierarchy edges for: {title}")
        cypher = """
        UNWIND $batch AS row
        MATCH (c:Paragraph {id: row.child_id})
        FOREACH (_ IN CASE WHEN row.parent = row.title THEN [1] ELSE [] END |
            MERGE (p:Title {name: row.parent})
            MERGE (c)-[:LINK]->(p)
        )
        FOREACH (_ IN CASE WHEN row.parent <> row.title THEN [1] ELSE [] END |
            MERGE (p:Heading {title: row.title, heading: row.parent})
            MERGE (c)-[:LINK]->(p)
        )
        """
        client.run_cypher_batches(cypher, article_data['hierarchy'], label='hierarchy')

    if article_data['links']:
        logging.info(f"Writing {len(article_data['links'])} link edges for: {title}")
        cypher = """
        UNWIND $batch AS row
        MATCH (a:Paragraph {id: row.from_id})
        MERGE (b:Title {name: row.to_title})
        MERGE (a)-[:LINK]->(b)
        """
        client.run_cypher_batches(cypher, article_data['links'], label='links')


def main():
    parser = argparse.ArgumentParser(description='Build knowledge graph into Neo4j')
    parser.add_argument('--articles-dir', default=DEFAULT_CONFIG.articles_dir)
    parser.add_argument('--neo4j-uri', default=DEFAULT_CONFIG.neo4j_uri)
    parser.add_argument('--neo4j-user', default=DEFAULT_CONFIG.neo4j_user)
    parser.add_argument('--neo4j-password', default=DEFAULT_CONFIG.neo4j_password)
    parser.add_argument('--batch-size', type=int, default=DEFAULT_CONFIG.batch_size)
    args = parser.parse_args()

    # instantiate an embedding client (deterministic fallback) and pass into Neo4j client
    embedder = EmbeddingClient(dim=DEFAULT_CONFIG.embedding_dim)
    client = Neo4jClient(uri=args.neo4j_uri, user=args.neo4j_user, password=args.neo4j_password, batch_size=args.batch_size, embedder=embedder, embed_dim=DEFAULT_CONFIG.embedding_dim)
    try:
        client.ensure_constraints()
        for fname in os.listdir(args.articles_dir):
            if not fname.endswith('.txt'):
                continue
            fpath = os.path.join(args.articles_dir, fname)
            logging.info(f"Processing {fpath}")
            article = parse_article(fpath)
            write_article_to_neo4j(client, article)
            # After paragraphs are written, compute + store embeddings for paragraph chunks
            if article.get('paragraphs'):
                client.write_paragraph_embeddings(article['paragraphs'])
    finally:
        client.close()


if __name__ == '__main__':
    main()
