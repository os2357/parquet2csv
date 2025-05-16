package main

import (
	"csv2parquet/internal/file"
	"csv2parquet/internal/helper"
	"csv2parquet/internal/schema"
	"flag"
	"log"
	"path/filepath"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	startTime := time.Now()
	var (
		err        error
		header     []string
		structType interface{}
		processor  schema.Processor
		pw         *writer.ParquetWriter
	)

	compression, delimiter, flush, table, verbose, csvFile, parquetFile := getParams()
	if _, err = file.IsExist(csvFile); err != nil {
		log.Fatalln(err.Error())
	}
	if _, err = file.IsWritable(filepath.Dir(parquetFile)); err != nil {
		log.Fatal(err.Error())
	}
	fw, err := local.NewLocalFileWriter(parquetFile)
	if err != nil {
		log.Fatalf("Can't create local file: %v", err)
	}
	i := 0
	bp := file.NewBatchProcessor(csvFile, 10000, []rune(*delimiter)[0], false)
	bCh, eCh := bp.Reader()
	for rows := range bCh {
		select {
		case err := <-eCh:
			log.Fatalf("Write error: %v", err)
		default:
			for _, rec := range rows.Rows {
				if i == 0 {
					header = rec
					structType, processor = schema.MatchSchema(*table, header)
					pw, err = writer.NewParquetWriter(fw, structType, 2) //nolint:mnd // maybe the number of threads
					if err != nil {
						log.Fatalf("Can't create parquet writer: %v", err)
					}
					pw.RowGroupSize = 128 * 1024 * 1024                                //nolint:mnd // 128MB
					pw.CompressionType = parquet.CompressionCodec(int32(*compression)) //nolint:gosec // compression >= 0
					i++
					continue
				}

				eData := processor(rec, structType, header)
				if err = pw.Write(eData); err != nil {
					log.Fatalf("Write error: %v", err)
				}

				if i == *flush {
					if err = pw.Flush(true); err != nil {
						log.Fatalf("Write Flush error: %v", err)
					}
					i = 0
				}
				i++
			}
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Fatalf("WriteStop error: %v", err)
	}
	err = fw.Close()
	if err != nil {
		log.Fatalf("Close Writer error: %v", err)
	}
	if *verbose {
		log.Printf("%s\n", helper.RuntimeStatistics(startTime, csvFile))
	}
}

func getParams() (*int, *string, *int, *string, *bool, string, string) {
	compression := flag.Int("compression", int(parquet.CompressionCodec_SNAPPY), "Compression codec: 0=UNCOMPRESSED, 1=SNAPPY, 2=GZIP")
	delimiter := flag.String("delimiter", ",", "CSV delimiter")
	flush := flag.Int("flush", 10000, "Flush every n rows")
	table := flag.String("table", "default", "Schema processing mode")
	verbose := flag.Bool("verbose", false, "Verbose output")
	flag.Parse()

	if len(flag.Args()) < 2 {
		log.Fatalf("Usage: ./csv2parquet [flags] <input.csv> <output.parquet>")
	}
	return compression, delimiter, flush, table, verbose, flag.Args()[0], flag.Args()[1]
}
