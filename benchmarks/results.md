# LSM vs B+Tree Benchmark (N=50,000 keys, ~2.5 MB logical)

| Metric | B+Tree (heap) | LSM-tree |
|---|---|---|
| Write throughput (ops/s) | 45,922 | 196,649 |
| Point read — hit (µs) | 5.08 | 22.19 |
| Point read — miss (µs) | 0.24 | 2.93 |
| Space amplification | 1.09x | 1.33x |
| Write amplification | 1.00x | 2.93x |
| Compactions | 0 | 2 |
| Bloom-filter skips (5k misses) | 0 | 23219 |
