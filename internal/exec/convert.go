package exec

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"reflect"
	"strconv"
	"strings"
)

var (
	ifaceType  = reflect.TypeOf(new(interface{}))
	intType    = reflect.TypeOf(int(0))
	stringType = reflect.TypeOf("")
	boolType   = reflect.TypeOf(true)
	bytesType  = reflect.TypeOf([]byte{})
	listType   = reflect.TypeOf([]interface{}{})
	mapType    = reflect.TypeOf(map[string]interface{}{})
)

//Convert converts attribute to  relect type
func Convert(attributeType string) reflect.Type {
	switch attributeType {
	case "N":
		return intType
	case "B":
		return bytesType
	case "S":
		return stringType
	case "BOOL":
		return boolType
	case "SS":
		return reflect.SliceOf(stringType)
	case "NS":
		return reflect.SliceOf(intType)
	case "BS":
		return reflect.SliceOf(stringType)
	case "L":
		return listType
	case "M":
		return reflect.SliceOf(mapType)
	case "NULL":
		return ifaceType
	}
	return nil
}

func databaseAttributeType(databaseType string) (string, error) {
	switch strings.ToLower(databaseType) {
	case "int", "numeric", "decimal":
		return "N", nil
	case "bool":
		return "BOOL", nil
	case "varchar", "text", "string":
		return "S", nil
	}
	return "", fmt.Errorf("unsupported key type: %v", databaseType)
}

func rawValue(dec *gojay.Decoder) ([]byte, error) {
	var value gojay.EmbeddedJSON
	err := dec.EmbeddedJSON(&value)
	if err != nil {
		return nil, err
	}
	if value[0] == '"' {
		value = value[1 : len(value)-1]
	}
	return value, nil
}

//asString converts dec input to string
func asString(dec *gojay.Decoder) (interface{}, error) {
	literal, err := rawValue(dec)
	if err != nil {
		return "", err
	}
	return string(literal), err
}

//asString converts dec input to []string
func asStrings(dec *gojay.Decoder) (interface{}, error) {
	strings := stringSlice{}
	return []string(strings), dec.Array(&strings)
}

//asBool converts dec input to bool
func asBool(dec *gojay.Decoder) (interface{}, error) {
	value := false
	return value, dec.Bool(&value)
}

//asInt converts dec input to int
func asInt(dec *gojay.Decoder) (interface{}, error) {
	literal, err := rawValue(dec)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(string(literal))
}

//asInts converts dec input to []int
func asInts(dec *gojay.Decoder) (interface{}, error) {
	literals := stringSlice{}
	err := dec.Array(&literals)
	if err != nil {
		return nil, err
	}
	var result = make([]int, len(literals))
	for i, item := range literals {
		v := strings.Trim(item, `"`)
		if result[i], err = strconv.Atoi(v); err != nil {
			return nil, err
		}
	}
	return result, nil
}

//asFloat64 converts dec input to float64
func asFloat64(dec *gojay.Decoder) (interface{}, error) {
	literal, err := rawValue(dec)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(string(literal), 64)
}

//asBytes converts dec input to []byte
func asBytes(dec *gojay.Decoder) (interface{}, error) {
	return rawValue(dec)
}

func decoderFor(t reflect.Type, required bool) (func(dec *gojay.Decoder) (interface{}, error), error) {
	switch t.Kind() {
	case reflect.Int:
		return asInt, nil
	case reflect.Float64:
		return asFloat64, nil
	case reflect.String:
		return asString, nil
	case reflect.Bool:
		return asBool, nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Int:
			return asInts, nil
		case reflect.String:
			return asStrings, nil
		case reflect.Uint8:
			return asBytes, nil

		}
	}
	return nil, fmt.Errorf("unsupported type: %s", t.String())
}
