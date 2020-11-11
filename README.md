# KeyperDB

### Overview
This project is naive implementation of disk-backed key-value store.
Database offers three operations `get`, `put` and `delete`
All of them (in theory) having O(1) complexity.

#### Implementation
Database is append-only log, constructed of 64kb pages.
All write operations (`put`, `delete`) are appended to the last page.
Database internally uses in-memory hash index, rebuild on each run.
Background job compacts the pages on schedule.

Implementation is not thread safe, as last page index is updated only on startup and by write operations going through current database instance.
So none of the writes going through other db instances will not be reflected within the index.
Compacting is also not thread safe.

#### Possible improvements
* replacing append log with Sorted String Table (SSTable)
