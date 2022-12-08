package dynamodb

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/dyndb/internal/exec"
	"reflect"
	"strings"
)

//FieldType represents a field type
type FieldType struct {
	Type reflect.Type
}

//UnmarshalJSONObject unmarshal object
func (s *FieldType) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	var err error
	s.Type = exec.Convert(k)
	if k == "N" {
		value := ""
		if err = dec.String(&value); err != nil {
			return err
		}
		if strings.Contains(value, ".") {
			s.Type = reflect.TypeOf(0.0)
		}
	}
	return nil
}

//NKeys returns keys count
func (s *FieldType) NKeys() int {
	return 0
}
