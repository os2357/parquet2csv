package schema

import (
	"encoding/json"

	"github.com/iancoleman/strcase"
	dynamicstruct "github.com/ompluscator/dynamic-struct"
)

type Processor func(record []string, sc interface{}, header []string) interface{}

func MatchSchema(sch string, header []string) (interface{}, Processor) {
	switch sch {
	case "default":
		return ProcessDefault(header)
	case "enrich":
		return new(EnrichData), ProcessEnrichData
	}
	panic("Schema not found")
}

func ProcessDefault(header []string) (interface{}, Processor) {
	sc := MakeDefaultSchema(header)
	return sc, func(record []string, sc interface{}, header []string) interface{} {
		data := make(map[string]interface{})
		if len(header) != len(record) {
			panic("header and record length not equal")
		}
		for i := range header {
			data[header[i]] = record[i]
		}
		jsonString, _ := json.Marshal(data)

		err := json.Unmarshal(jsonString, &sc)
		if err != nil {
			panic(err)
		}
		return sc
	}
}

func MakeDefaultSchema(header []string) interface{} {
	sc := dynamicstruct.NewStruct()
	for i := range header {
		sc.AddField(
			strcase.ToCamel(header[i]),
			"",
			`json:"`+header[i]+`" parquet:"name=`+header[i]+`, type=BYTE_ARRAY, convertedtype=UTF8"`,
		)
	}
	return sc.Build().New()
}
