"""A classic Bloom filter.

Used by each SSTable so a point lookup can skip tables that *definitely* do not
contain the key, turning most negative lookups into zero disk reads. False
positives are possible (we then read and miss); false negatives are not.
"""

import hashlib
import math


class BloomFilter:
    def __init__(self, n_items: int, fp_rate: float = 0.01):
        n = max(1, n_items)
        # Optimal bit count m and hash count k for the target false-positive rate.
        self.m = max(8, int(-(n * math.log(fp_rate)) / (math.log(2) ** 2)))
        self.k = max(1, int((self.m / n) * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)

    def _hashes(self, key: bytes):
        h = hashlib.sha256(key).digest()
        h1 = int.from_bytes(h[:8], "little")
        h2 = int.from_bytes(h[8:16], "little")
        for i in range(self.k):
            yield (h1 + i * h2) % self.m       # double hashing

    def add(self, key: bytes) -> None:
        for bit in self._hashes(key):
            self.bits[bit >> 3] |= (1 << (bit & 7))

    def maybe_contains(self, key: bytes) -> bool:
        for bit in self._hashes(key):
            if not (self.bits[bit >> 3] & (1 << (bit & 7))):
                return False                   # definitely absent
        return True                            # possibly present

    # serialization for persisting alongside an SSTable
    def to_dict(self) -> dict:
        return {"m": self.m, "k": self.k, "bits": bytes(self.bits)}

    @classmethod
    def from_dict(cls, d: dict) -> "BloomFilter":
        bf = cls.__new__(cls)
        bf.m, bf.k, bf.bits = d["m"], d["k"], bytearray(d["bits"])
        return bf
