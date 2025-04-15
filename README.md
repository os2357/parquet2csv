# csv2parquet

[![Go Reference](https://pkg.go.dev/badge/golang.org/x/example.svg)](https://pkg.go.dev/golang.org/x/example) ![example workflow](https://github.com/dbunt1tled/parquet2csv/actions/workflows/go.yml/badge.svg)

A fast, reliable CLI tool for converting CSV files to Apache Parquet format. Built in Go, it’s designed for data workflows that need efficient, schema-aware columnar storage.

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
  --flush int
        number of rows to flush (default 10000)
  --help
        Show this help message
  --schema string
        schema of csv file (default "default")
  --compression int
        Type of compression (default 0)
  --verbose
        Statistic info in the end
  <input file path>
  <output file path>
```