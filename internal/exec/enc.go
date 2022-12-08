package exec

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/francoispqt/gojay"
	"strconv"
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
