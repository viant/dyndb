package exec

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/francoispqt/gojay"
	"strconv"
	"strings"
)

var encoder = attributevalue.NewEncoder()

//Encode encodes value
func Encode(value interface{}) (types.AttributeValue, error) {
	switch actual := value.(type) {
	case []string:
		return &types.AttributeValueMemberSS{Value: actual}, nil
	case []int:
		var values = make([]string, len(actual))
		for i, v := range actual {
			values[i] = strconv.Itoa(v)
		}
		return &types.AttributeValueMemberNS{Value: values}, nil
	case []float64:
		var values = make([]string, len(actual))
		for i, v := range actual {
			values[i] = strconv.FormatFloat(v, 'f', 10, 64)
		}
		return &types.AttributeValueMemberNS{Value: values}, nil
	}
	return encoder.Encode(value)
}

//ints transient type
type ints []int

// UnmarshalJSONArray decodes JSON array elements into slice
func (a *ints) UnmarshalJSONArray(dec *gojay.Decoder) error {
	var value int
	if err := dec.Int(&value); err != nil {
		return err
	}
	*a = append(*a, value)
	return nil
}

// MarshalJSONArray encodes arrays into JSON
func (a ints) MarshalJSONArray(enc *gojay.Encoder) {
	for i := 0; i < len(a); i++ {
		enc.Int(a[i])
	}
}

// IsNil checks if array is nil
func (a ints) IsNil() bool {
	return len(a) == 0
}

type stringSlice []string

// UnmarshalJSONArray decodes JSON array elements into slice
func (a *stringSlice) UnmarshalJSONArray(dec *gojay.Decoder) error {
	var value string
	if err := dec.String(&value); err != nil {
		return err
	}
	*a = append(*a, value)
	return nil
}

// MarshalJSONArray encodes arrays into JSON
func (a stringSlice) MarshalJSONArray(enc *gojay.Encoder) {
	for i := 0; i < len(a); i++ {
		enc.String(a[i])
	}
}

// IsNil checks if array is nil
func (a stringSlice) IsNil() bool {
	return len(a) == 0
}

//ints transient type
type object struct {
	m map[string]interface{}
}

func (o *object) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	if len(o.m) == 0 {
		o.m = map[string]interface{}{}
	}
	wrapper := &wrapper{}
	err := dec.Object(wrapper)
	o.m[k] = wrapper.value
	return err
}

// NKeys returns the number of keys to unmarshal
func (t *object) NKeys() int { return 0 }

//ints transient type
type wrapper struct {
	value interface{}
}

func (w *wrapper) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	var err error
	switch k {
	case "S":
		value := ""
		err = dec.String(&value)
		w.value = value
		return err
	case "SS":
		value := stringSlice{}
		err = dec.Array(&value)
		w.value = value
		return err
	case "NS":
		value := ints{}
		err = dec.Array(&value)
		w.value = value
		return err
	case "N":
		value := ""
		err := dec.String(&value)
		if strings.Contains(value, ".") {
			w.value, err = strconv.ParseFloat(value, 64)
			return err
		}
		w.value, err = strconv.Atoi(value)
		return err
	case "BOOL":
		value := false
		err = dec.Bool(&value)
		w.value = value
		return err
	case "L":
		l := list{}
		err = dec.Array(&l)
		w.value = l.items
		return err
	case "M":
		o := object{}
		err := dec.Object(&o)
		w.value = o.m
		return err
	}
	var embeded gojay.EmbeddedJSON
	if err := dec.EmbeddedJSON(&embeded); err != nil {
		return err
	}
	var value interface{}
	err = json.Unmarshal(embeded, &value)
	w.value = value
	return err
	return nil
}

// NKeys returns the number of keys to unmarshal
func (w *wrapper) NKeys() int { return 0 }

//ints transient type
type list struct {
	object *wrapper
	items  []interface{}
}

// NKeys returns the number of keys to unmarshal
func (t *list) NKeys() int { return 0 }

// UnmarshalJSONArray decodes JSON array elements into slice
func (a *list) UnmarshalJSONObject(dec *gojay.Decoder) error {
	return dec.Array(a)
}

// UnmarshalJSONArray decodes JSON array elements into slice
func (a *list) UnmarshalJSONArray(dec *gojay.Decoder) error {
	err := dec.Object(a.object)
	a.items = append(a.items, a.object.value)
	return err
}
