package dynamodb

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/dyndb/internal/exec"
)

//Output represents optimized output
type Output struct {
	Type  *exec.Type
	Rows  []Region
	Data  []byte
	Field *exec.Field
	FieldType
}

//UnmarshalJSONObject unmarshal object
func (s *Output) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	s.Field = s.Type.Field(k)
	var err error
	if s.Field.Type == nil {
		err = dec.Object(&s.FieldType)
		s.Field.Type = s.FieldType.Type
	}
	return err
}

func adjustArrayItemEndPosition(data []byte) int {
	if len(data) == 0 || data[0] == '}' {
		return 0
	}
	for i := len(data) - 1; i >= 0; i-- {
		switch data[i] {
		case ' ', '\n', '\t':
		case ',':
			return len(data) - i
		}
	}
	return 0
}

//NKeys returns keys
func (s *Output) NKeys() int {
	return 0
}

//UnmarshalJSONArray unmrshal array
func (s *Output) UnmarshalJSONArray(dec *gojay.Decoder) error {
	decCursor := cursor(dec)
	region := Region{Begin: decCursor}
	err := dec.Object(s)
	rowsLen := len(s.Rows)
	if rowsLen > 0 {
		prevRegion := &s.Rows[rowsLen-1]
		prevRegion.End = region.Begin
		adj := adjustArrayItemEndPosition(s.Data[prevRegion.Begin:prevRegion.End])
		prevRegion.End -= adj
	}
	s.Rows = append(s.Rows, region)
	return err
}
