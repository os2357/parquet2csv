package schema

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bytedance/sonic"
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
		jsonString, _ := sonic.ConfigFastest.Marshal(data)

		err := sonic.ConfigFastest.Unmarshal(jsonString, &sc)
		if err != nil {
			panic(err)
		}
		return sc
	}
}

func sanitizeFieldName(s string, i int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return fmt.Sprintf("Field%d", i)
	}
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	s = re.ReplaceAllString(s, "_")
	s = strings.TrimLeft(s, "_")
	if s == "" || (s[0] < 'A' || (s[0] > 'Z' && s[0] < 'a') || s[0] > 'z') {
		s = fmt.Sprintf("Field%d_%s", i, s)
	}
	return strcase.ToCamel(s)
}

func MakeDefaultSchema(header []string) interface{} {
	sc := dynamicstruct.NewStruct()
	for i := range header {
		original := header[i]
		fieldName := sanitizeFieldName(original, i)

		// Final check
		if fieldName == "" {
			fieldName = fmt.Sprintf("Field%d", i)
		}

		// Escape quotes in tags just in case
		cleanTag := strings.ReplaceAll(original, `"`, "")
		fmt.Printf("Header %d: original=%q → sanitized=%q\n", i, original, fieldName)

		sc.AddField(
			fieldName,
			"string",
			fmt.Sprintf(`json:"%s" parquet:"name=%s, type=BYTE_ARRAY, convertedtype=UTF8"`, cleanTag, cleanTag),
		)
	}
	return sc.Build().New()
}
