# multiproc_tools

tool classes for multiprocessing (SharedTupleList)

## performance
The examples are pretty slow!
See: https://medium.com/@rvprasad/performance-of-system-v-style-shared-memory-support-in-python-3-8-d7a7d1b1fb96
In responses Davin explains the performance issues with ShareableList with O(n) complexity for read-/write-access.
Optimization could be numpy.arrays.
