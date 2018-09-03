# go-db-benchmark-app-sub-query
database benchmark between app query and SQL sub query in golang [sqlx](https://github.com/jmoiron/sqlx)

See https://sligrid.com/tsaikd/grid/61b41ff8-af42-11e8-b75e-af9bec6f16d3 for result

# Get connection certificate from google cloud SQL

* client-cert.pem
* client-key.pem
* server-ca.pem

# Create table

[create_table.sql](create_table.sql)

# Insert seed data

```
export MYSQL_URL="USER:PASS@tcp(IP:PORT)/DBNAME?tls=custom"
export PGSQL_URL="postgresql://USER:PASS@IP/DBNAME?sslmode=require"
go run seed.go
```

# Run benchmark

```
export MYSQL_URL="USER:PASS@tcp(IP:PORT)/DBNAME?tls=custom"
export PGSQL_URL="postgresql://USER:PASS@IP/DBNAME?sslmode=require"
go test -v -timeout=10m -benchmem -run=^$ -bench ^Benchmark
```
