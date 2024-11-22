package main

import (
	"csv2parquet/internal/file"
	"csv2parquet/internal/helper"
	"csv2parquet/internal/schema"
	"encoding/csv"
	"errors"
	"flag"
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
		header []string
	)

	compression := flag.Int("compr3ession", 0, "Type of compression")
	delimiter := flag.String("delimiter", ",", "Delimiter for csv file")
	flush := flag.Int("flush", 10000, "number of rows to flush")
	table := flag.String("schema", "default", "schema of csv file")
	verbose := flag.Bool("verbose", false, "Show this help message")
	help := flag.Bool("help", false, "Show this help message")
	flag.Parse()
	helper.AppHelp(*help)
	for _, arg := range os.Args[1:] {
		if arg[0] == '-' {
			continue
		}
		args = append(args, arg)
	}
	if len(args) < 2 { //nolint:mnd // 2 required params
		log.Fatal("Usage: ./cvs2parquet <file.csv> <file.parquet>")
	}
	csvFile := args[0]
	parquetFile := args[1]

	if _, err = file.IsExist(csvFile); err != nil {
		log.Fatal(err.Error())
	}
	if _, err = file.IsWritable(filepath.Dir(parquetFile)); err != nil {
		log.Fatal(err.Error())
	}
	cFile, err := os.Open(csvFile)
	if err != nil {
		log.Fatal(err.Error())
	}
	parser := csv.NewReader(cFile)
	fw, err := local.NewLocalFileWriter(parquetFile)
	if err != nil {
		log.Fatal("Can't create local file" + err.Error())
	}

	d := *delimiter
	parser.Comma = []rune(d)[0]
	header, err = parser.Read()
	if err != nil {
		log.Fatal(err.Error())
	}
	i := 0

	structType, processor := schema.MatchSchema(*table, header)
	pw, err := writer.NewParquetWriter(fw, structType, 4) //nolint:mnd // maybe the number of threads
	if err != nil {
		log.Fatal("Can't create parquet writer" + err.Error())
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec(int32(*compression))

	for {
		record, err = parser.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatal(err.Error())
		}
		eData = processor(record, structType, header)

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
		log.Fatal("WriteFlush error: " + err.Error())
	}
	if err = pw.WriteStop(); err != nil {
		log.Fatal("WriteStop error: " + err.Error())
	}

	if err = fw.Close(); err != nil {
		log.Fatal("Write Finish error: " + err.Error())
	}
	if *verbose {
		log.Printf("%s\n", helper.RuntimeStatistics(startTime))
	}
}
