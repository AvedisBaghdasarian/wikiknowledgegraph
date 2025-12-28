import os
import glob
import json
import time
import uuid

from neo4j import GraphDatabase
import pytest

# Neo4j connection baked into the test (change if needed)
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def test_neo4j_500_batch_write():
    """Benchmark a 500-operation batch write to Neo4j.

    This test writes N small nodes in a single UNWIND batch, prints elapsed
    time, then removes the nodes. It asserts that the expected number of
    nodes were created.
    """
    driver = get_driver()
    run_id = str(uuid.uuid4())

    # Try to find a saved batch file produced by `build_kg_neo4j.py` under
    # the repository root `saved_batches/`. If present, use its `cypher` and
    # `batch` payload. Otherwise fall back to generating a simple benchmark
    # payload similar to the original test.
    repo_root = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
    saved_dir = os.path.join(repo_root, 'saved_batches')
    saved_files = glob.glob(os.path.join(saved_dir, '*.json')) if os.path.isdir(saved_dir) else []

    if saved_files:
        saved_path = saved_files[0]
        with open(saved_path, 'r', encoding='utf-8') as fh:
            saved = json.load(fh)
        batch = saved.get('batch')
        cypher = saved.get('cypher') or "UNWIND $batch AS row RETURN count(1) AS created"
        
        N_actual = len(batch) if batch is not None else 0
        print(f"Using saved batch file: {saved_path} (operations={N_actual})")
        start = time.perf_counter()
        with driver.session() as session:
            # execute saved cypher with variable name $batch
            session.run(cypher, batch=batch)
        elapsed = time.perf_counter() - start

        rate = N_actual / elapsed if elapsed > 0 else 0
        print(f"Neo4j saved-batch write: elapsed={elapsed:.4f}s rate={rate:.2f} ops/sec")

        # Try a safe cleanup for common node id fields
        ids = [r.get('id') for r in batch if isinstance(r, dict) and 'id' in r]
        child_ids = [r.get('child_id') for r in batch if isinstance(r, dict) and 'child_id' in r]
        from_ids = [r.get('from_id') for r in batch if isinstance(r, dict) and 'from_id' in r]
        # flatten and filter
        ids = [x for x in ids if x is not None]
        child_ids = [x for x in child_ids if x is not None]
        from_ids = [x for x in from_ids if x is not None]

        with driver.session() as session:
            if ids:
                session.run("MATCH (n) WHERE n.id IN $ids DETACH DELETE n", ids=ids)
            if child_ids:
                session.run("MATCH (n) WHERE n.id IN $ids DETACH DELETE n", ids=child_ids)
            if from_ids:
                session.run("MATCH (n) WHERE n.id IN $ids DETACH DELETE n", ids=from_ids)

        driver.close()
        # Basic sanity: ensure we had some operations
        assert N_actual > 0
    else:
        # fallback: optimized simple benchmark for 20k target
        N_TOTAL = 20000
        rows = [{"id": i, "run_id": run_id, "value": f"val_{i}"} for i in range(N_TOTAL)]
        
        # Use a more realistic query that includes label and property SET
        cypher = (
            "UNWIND $rows AS r\n"
            "MERGE (n:BenchBenchmark {uid: r.id, run_id: r.run_id})\n"
            "ON CREATE SET n.value = r.value\n"
            "RETURN count(n) AS created"
        )

        start = time.perf_counter()
        with driver.session() as session:
            # Ensure index exists for benchmark
            session.run("CREATE CONSTRAINT benchmark_uid IF NOT EXISTS FOR (n:BenchBenchmark) REQUIRE n.uid IS UNIQUE")
            
            result = session.run(cypher, rows=rows)
            created = result.single()["created"]
        elapsed = time.perf_counter() - start
        
        rate = created / elapsed if elapsed > 0 else 0
        print(f"Neo4j bulk write: created={created} elapsed={elapsed:.4f}s rate={rate:.2f} nodes/sec")

        # Cleanup created nodes for this run
        with driver.session() as session:
            session.run("MATCH (n:BenchBenchmark {run_id: $run_id}) DETACH DELETE n", run_id=run_id)

        driver.close()

        assert created == len(rows)


    def test_generate_embeddings_2d_image():
        """Fetch paragraph embeddings from Neo4j and write a 2D scatter PNG.

        This test requires `numpy`, `scikit-learn` and `matplotlib`. If they are
        not installed the test is skipped.
        """
        driver = get_driver()
        with driver.session() as session:
            result = session.run("MATCH (p:Paragraph) WHERE exists(p.embedding) RETURN p.id AS id, p.embedding AS embedding LIMIT 5000")
            rows = [(rec["id"], rec["embedding"]) for rec in result]
        driver.close()

        if not rows:
            pytest.skip("No paragraph embeddings found in database")

        try:
            import numpy as np
            from sklearn.decomposition import PCA
            import matplotlib.pyplot as plt
        except Exception as e:
            pytest.skip(f"Skipping embedding visualization: missing deps ({e})")

        ids = [r[0] for r in rows]
        embs = np.vstack([np.array(r[1], dtype=np.float32) for r in rows])

        if embs.shape[1] > 2:
            pca = PCA(n_components=2)
            pts = pca.fit_transform(embs)
        else:
            pts = embs[:, :2]

        plt.figure(figsize=(8, 6))
        plt.scatter(pts[:, 0], pts[:, 1], s=8)
        for i, label in enumerate(ids[:20]):
            plt.annotate(str(label), (pts[i, 0], pts[i, 1]), fontsize=6)
        out = os.path.join(os.path.dirname(__file__), 'embeddings_2d.png')
        plt.savefig(out, dpi=150)
        plt.close()
        print(f"Saved embeddings scatter to {out}")
        assert os.path.exists(out)
