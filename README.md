# simple-db

Implementation of the SimpleDB database system in Go.

http://www.cs.bc.edu/~sciore/simpledb/

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

See below files:
- [internal/testdata/example.sql](internal/testdata/example.sql)
- [internal/postgres/server_test.go](internal/postgres/server_test.go)