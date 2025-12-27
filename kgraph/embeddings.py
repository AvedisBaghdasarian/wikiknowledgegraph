"""Simple embedding helper.

This module provides a deterministic fallback embedding generator that maps
text -> fixed-length vector. It avoids external dependencies so tests can run
without API keys. If you want to plug in a real provider, replace
`get_embedding` implementation accordingly.
"""
from hashlib import sha256
from typing import List


class EmbeddingClient:
    def __init__(self, dim: int = 1536):
        self.dim = dim

    def get_embedding(self, text: str) -> List[float]:
        """Deterministically generate a vector of length `self.dim` from `text`.

        Uses repeated SHA256 hashing to emit pseudo-random but stable floats
        in range [-1.0, 1.0]. This is sufficient for testing, visualization
        and demonstrating vector storage/querying in Neo4j.
        """
        vec = []
        i = 0
        # produce enough bytes to convert into float values
        while len(vec) < self.dim:
            h = sha256(f"{text}\x00{i}".encode('utf-8')).digest()
            # split digest into 8-byte chunks -> uint64 -> float in [-1,1]
            for j in range(0, len(h), 8):
                chunk = h[j:j+8]
                if not chunk:
                    continue
                val = int.from_bytes(chunk, 'big')
                # max for 8 bytes is 2**64-1
                f = (val / 2**64) * 2.0 - 1.0
                vec.append(float(f))
                if len(vec) >= self.dim:
                    break
            i += 1
        return vec[:self.dim]
