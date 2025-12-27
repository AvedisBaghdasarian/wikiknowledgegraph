## Purpose
This file gives focused, actionable guidance for AI coding agents working in this repository. Keep suggestions concrete and tied to the existing pipeline and files.

**Overview**
- **Pipeline:** The repo transforms a Wikipedia XML dump into per-article text files and then builds a Neo4j knowledge graph. Key steps: parse XML -> save article .txt files -> ingest into Neo4j.
- **Primary scripts:** [extract_articles.py](/extract_articles.py#L1-L120) (XML -> articles_output), [build_kg_neo4j.py](/build_kg_neo4j.py#L1-L200) (articles_output -> Neo4j), and [docker-compose.yml](/docker-compose.yml#L1-L60) (Neo4j service).

**Quick start / common commands**
```bash
# Start Neo4j (foreground)
docker compose up neo4j
# Or in background
docker compose up -d neo4j

# Extract a small set of articles (edit MAX_ARTICLES in extract_articles.py first)
python3 extract_articles.py

# Run import into Neo4j
python3 build_kg_neo4j.py

# Tail Neo4j logs
docker logs -f neo4j
```

**Project-specific conventions & patterns**
- **File output:** Articles are written to the `articles_output` directory using `sanitize_filename` and a 100-char filename limit; filenames may contain underscores.
- **Heading detection:** `build_kg_neo4j.py` uses the regex ^(=+)([^=]+)=+$ to detect headings and determines hierarchy by heading level. See [build_kg_neo4j.py](/build_kg_neo4j.py#L20-L60).
- **Paragraph chunking:** Text is split with `split_paragraphs` using `MAX_PARAGRAPH_LEN` and `PARA_OVERLAP`. Chunk ids follow the pattern `title:line:chunk_idx` (see `split_paragraphs` and paragraph creation in [build_kg_neo4j.py](/build_kg_neo4j.py#L1-L120)).
- **Wikilink extraction:** Wikilinks are found with the pattern `\[\[([^\]|#]+)` and converted to `LINKS_TO` edges to `Title` nodes.
- **Batch writes:** All writes use `batch_write` with `BATCH_SIZE` to group Cypher operations. Tune `BATCH_SIZE` in `build_kg_neo4j.py` for performance.

**Neo4j / integration notes**
- Default Neo4j Bolt endpoint: `bolt://localhost:7687`. Credentials are set in `build_kg_neo4j.py` (NEO4J_USER / NEO4J_PASSWORD) and in `docker-compose.yml` via `NEO4J_AUTH`. Update both places when changing credentials.
- Docker volumes in `docker-compose.yml` persist DB data; be mindful when resetting state: remove `neo4j_data` volume to start fresh.

**Editing guidance for changes**
- When adding/renaming node labels or relationships, update both the ingestion Cypher strings and any code that matches nodes (e.g., `MATCH (p:Paragraph {id: ...})`).
- Preserve existing id formats (`Title`, `Heading`, `Paragraph`) to avoid breaking back-compat with earlier imports.

**Debugging & testing tips**
- To debug scale or parsing errors, set `MAX_ARTICLES` in [extract_articles.py](/extract_articles.py#L1-L120) to a small number and run end-to-end.
- Use Python logging output from both scripts; the code logs high-level progress (file saved, batches written).
- If Neo4j connection fails, check `NEO4J_AUTH` in [docker-compose.yml](/docker-compose.yml#L1-L40) and ensure the container is running and ports 7474/7687 are reachable.

**Files to inspect for domain knowledge**
- [extract_articles.py](/extract_articles.py#L1-L120) — XML parsing and file-output rules.
- [build_kg_neo4j.py](/build_kg_neo4j.py#L1-L220) — heading parsing, paragraph chunking, Cypher strings, batching.
- [docker-compose.yml](/docker-compose.yml#L1-L60) — Neo4j runtime config and volumes.

If any of these areas need more detail (example Cypher queries, sample article text snippets, or performance tuning), tell me which section to expand.
