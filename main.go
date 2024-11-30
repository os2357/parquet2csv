package main

import (
	"csv2parquet/internal/file"
	"csv2parquet/internal/helper"
	"csv2parquet/internal/schema"
	"flag"
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
		err        error
		header     []string
		args       []string
		structType interface{}
		processor  schema.Processor
		pw         *writer.ParquetWriter
	)

	compression, delimiter, flush, table, verbose, csvFile, parquetFile := getParams(args)
	if _, err = file.IsExist(csvFile); err != nil {
		log.Fatal(err.Error())
		os.Exit(1)
	}
	if _, err = file.IsWritable(filepath.Dir(parquetFile)); err != nil {
		log.Fatal(err.Error())
	}
	fw, err := local.NewLocalFileWriter(parquetFile)
	if err != nil {
		log.Println("Can't create local file", err)
		os.Exit(1)
	}
	i := 0
	for rec := range file.ReadCSV(csvFile, []rune(*delimiter)[0], false) {
		if i == 0 {
			header = rec
			structType, processor = schema.MatchSchema(*table, header)
			pw, err = writer.NewParquetWriter(fw, structType, 2) //nolint:mnd // maybe the number of threads
			if err != nil {
				log.Println("Can't create parquet writer", err)
				os.Exit(1)
			}
			pw.RowGroupSize = 128 * 1024 * 1024                                //nolint:mnd // 128MB
			pw.CompressionType = parquet.CompressionCodec(int32(*compression)) //nolint:gosec // compression >= 0
			i++
			continue
		}

		eData := processor(rec, structType, header)
		if err = pw.Write(eData); err != nil {
			log.Println("Write error", err)
		}

		if i == *flush {
			if err = pw.Flush(true); err != nil {
				log.Fatal("WriteFlush error", err)
				os.Exit(1)
			}
			i = 0
		}
		i++
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		os.Exit(1)
	}
	err = fw.Close()
	if err != nil {
		log.Println("Close Writer error", err)
		os.Exit(1)
	}
	if *verbose {
		log.Printf("%s\n", helper.RuntimeStatistics(startTime, csvFile))
	}
}

func getParams(args []string) (*int, *string, *int, *string, *bool, string, string) {
	compression := flag.Int("compression", 0, "Type of compression")
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
	return compression, delimiter, flush, table, verbose, csvFile, parquetFile
}
