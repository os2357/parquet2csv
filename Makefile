run:
	@GOGC=150 @go run
build:
	@GOGC=150 @go build
build-opt:
	@GOGC=150 @go build -ldflags "-s -w"  -o csv2parquet