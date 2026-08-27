# simple-db

Implementation of the SimpleDB database system in Go.

http://www.cs.bc.edu/~sciore/simpledb/

## Architecture Overview

SimpleDB is a teaching database system that implements core database functionality with a PostgreSQL-compatible interface. Here's an overview of its key components:

### Core Components

1. **Storage Layer**
   - File management for block-level operations
   - Buffer pool for caching data blocks in memory
   - Record management and schema definitions
   - Write-ahead logging for transaction recovery

2. **Index Management**
   - B-tree indexing implementation
   - Support for creating and using indexes on table columns
   - Efficient index scan operations

3. **Query Processing**
   - SQL parser and lexer
   - Query planning and optimization with heuristic approach
   - Various join strategies including merge join
   - Support for materialization and sorting

4. **Transaction Management**
   - ACID compliance
   - BEGIN, COMMIT, and ROLLBACK support
   - Recovery mechanisms

5. **Metadata Management**
   - System catalogs management
   - Table and index definitions tracking

6. **Client Interface**
   - PostgreSQL wire protocol implementation
   - Compatible with standard `psql` client

### Features

- Basic SQL operations (CREATE TABLE, INSERT, SELECT, UPDATE, DELETE)
- JOIN operations and complex queries
- GROUP BY and aggregation
- Index-based query optimization
- Transaction management
- Block-based storage with buffer management

## Usage

Launch the SimpleDB server with the following command:

```sh
go run ./cmd/simpledb -dir "path/to/db"
```

Clients can connect to the server using the `psql` command line tool:

```sh
psql -h localhost -p 45432
```

## Supported SQL Commands / Examples

See below files for detailed SQL examples and usage:
- [internal/testdata/example.sql](internal/testdata/example.sql)
- [internal/postgres/server_test.go](internal/postgres/server_test.go)