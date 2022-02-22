**Migrated to [rdms](http://github.com/bnclabs/rdms) project.**

A tiny tool to measure disk performance for both HDD and SSD. By
performance we mean **latency** and **throughput**. The tool
can also plot latency and throught graphs.

Flush loop
==========

Flush loop is an independant thread continuously flushing fixed size
data-block. Data block is pre-populated with sample content and
after every write, file is sync-ed to disk.

**Latency** includes time taken to write() data to the file and
sync() file data and metadata to disk.

**Throughput** is measured as Mega-bytes of data written to disk
for every second.
