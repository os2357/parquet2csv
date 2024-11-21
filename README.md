# csv2parquet

[![Go Reference](https://pkg.go.dev/badge/golang.org/x/example.svg)](https://pkg.go.dev/golang.org/x/example)

Small app to convert csv to parquet

## Install & Run

```
$ go build
$ ./csv2parquet file.csv file.parquet
```

## Additional Flags

```
$ ./csv2parquet --help
Usage of ./csv2parquet:
  --delimiter string
        Delimiter for csv file (default ",")
  --compression int
        Type of compression (default 0)
  --flush int
        number of rows to flush (default 10000)
  --help
        Show this help message
  -schema string
        schema of csv file (default "default")
```
