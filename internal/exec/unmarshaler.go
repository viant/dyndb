package exec

import (
	"database/sql/driver"
	"github.com/francoispqt/gojay"
)

type (
	fieldUnmarshaler struct {
		values []driver.Value
		field  *Field
	}
)

// NKeys returns the number of keys to unmarshal
func (t *fieldUnmarshaler) NKeys() int { return 0 }

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (t *fieldUnmarshaler) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	value, err := t.field.Decoder(dec)
	if err != nil {
		return err
	}
	t.values[t.field.Pos] = value
	return nil
}
