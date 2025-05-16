# csv2parquet

A fast, reliable CLI tool for converting CSV files to Apache Parquet format. Designed for data workflows that need efficient, schema-aware columnar storage.

## Features

- Converts CSV to Parquet with configurable compression (UNCOMPRESSED, SNAPPY, GZIP)
- Automatically sanitizes CSV headers into valid Go struct field names
- Supports large row group sizes for efficient downstream reads
- Handles common encoding schemes and schema mapping patterns
- Useful for benchmarking, analytics, and data ingestion pipelines

## Install & Run

```bash
go build
./csv2parquet input.csv output.parquet
````

## Recommended Usage for Benchmarking

```bash
./csv2parquet -compression=2 -flush=10000 -delimiter="," input.csv output.parquet
```

* `-compression=2` enables **GZIP** compression (best size for benchmarking)
* `-flush=10000` flushes rows in large row groups (\~10K rows at a time)

## Command-Line Flags

```bash
./csv2parquet --help
```

| Flag            | Description                                                         | Default      |
| --------------- | ------------------------------------------------------------------- | ------------ |
| `--delimiter`   | Delimiter character for CSV fields                                  | `","`        |
| `--flush`       | Number of rows to flush per group                                   | `10000`      |
| `--schema`      | Schema processor (e.g. "default", "enrich")                         | `"default"`  |
| `--compression` | Parquet compression codec:<br> 0=UNCOMPRESSED<br>1=SNAPPY<br>2=GZIP | `1` (SNAPPY) |
| `--verbose`     | Show statistics/logging                                             | `false`      |

### Positional Arguments

```
<input file path>  Path to CSV file
<output file path> Path to output Parquet file
```

## Example

```bash
./csv2parquet -compression=2 data.csv data.parquet
```