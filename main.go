package main

import (
	"csv2parquet/internal/file"
	"csv2parquet/internal/helper"
	"csv2parquet/internal/shcema"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func main() {
	startTime := time.Now()
	var (
		err    error
		eData  interface{}
		args   []string
		record []string
	)

	compression := flag.Int("compression", 0, "Type of compression")
	delimiter := flag.String("delimiter", ",", "Delimiter for csv file")
	flush := flag.Int("flush", 10000, "number of rows to flush")
	schema := flag.String("schema", "default", "schema of csv file")
	help := flag.Bool("help", false, "Show this help message")
	flag.Parse()
	if *help {
		fmt.Println(`Usage of ./csv2parquet:
  --delimiter string
        Delimiter for csv file (default ",")
  --compression int
        Type of compression (default 0)
  --flush int
        number of rows to flush (default 10000)
  --help
        Show this help message
  -schema string
        schema of csv file (default "default")`)
		os.Exit(0)
	}
	for _, arg := range os.Args[1:] {
		if arg[0] == '-' {
			continue
		}
		args = append(args, arg)
	}
	if len(args) < 2 {
		panic("Usage: go run main.go <file.csv> <file.parquet>")
	}
	csvFile := args[0]
	parquetFile := args[1]

	if _, err = file.IsExist(csvFile); err != nil {
		panic(err)
	}
	if _, err = file.IsWritable(filepath.Dir(parquetFile)); err != nil {
		panic(err)
	}
	cFile, err := os.Open(csvFile)
	if err != nil {
		panic(err)
	}
	parser := csv.NewReader(cFile)
	fw, err := local.NewLocalFileWriter(parquetFile)
	if err != nil {
		panic("Can't create local file" + err.Error())
	}
	structType, processor := matchSchema(*schema)
	pw, err := writer.NewParquetWriter(fw, structType, 4)
	if err != nil {
		panic("Can't create parquet writer" + err.Error())
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec(int32(*compression))
	d := *delimiter
	parser.Comma = []rune(d)[0]
	_, err = parser.Read()
	if err != nil {
		panic(err)
	}
	i := 0
	for {
		record, err = parser.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		eData = processor(record)

		if err = pw.Write(&eData); err != nil {
			log.Println("Write error", err)
		}

		if i == *flush {
			if err = pw.Flush(true); err != nil {
				log.Println("WriteFlush error", err)
				return
			}
			i = 0
		}
		i++

	}
	if err = pw.Flush(true); err != nil {
		log.Println("WriteFlush error", err)
		return
	}

	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}

	if err = fw.Close(); err != nil {
		log.Println("Write Finish error", err)
		return
	}
	fmt.Println(helper.RuntimeStatistics(startTime))
}

func matchSchema(schema string) (interface{}, shcema.SchemaProcessor) {
	switch schema {
	case "default":
		return func(record []string) interface{} {
			return nil //TODO Denis: add default schema
		}, nil
	case "enrich":
		return new(shcema.EnrichData), shcema.ProcessEnrichData
	}
	panic("Schema not found")
}
