package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/francoispqt/gojay"
	"github.com/viant/dyndb/internal/exec"
)

//ExecuteStatementOutput statement output
type ExecuteStatementOutput struct {
	Encoder *attributevalue.Encoder
	*dynamodb.ExecuteStatementOutput
	*Output
}

// IsNil checks if instance is nil
func (o *ExecuteStatementOutput) IsNil() bool {
	return o == nil
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (o *ExecuteStatementOutput) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	switch k {
	case "Items":
		err := dec.Array(o.Output)
		if len(o.Output.Rows) > 0 {
			o.Output.Rows[len(o.Output.Rows)-1].End = cursor(dec) - 1
		}
		return err
	case "NextToken":
		var value string
		err := dec.String(&value)
		if err == nil {
			o.NextToken = &value
		}
		return err
	}
	return nil
}

// NKeys returns the number of keys to unmarshal
func (o *ExecuteStatementOutput) NKeys() int { return 0 }

//NewExecuteStatementOutput returns statement output
func NewExecuteStatementOutput(schemaType *exec.Type) *ExecuteStatementOutput {
	return &ExecuteStatementOutput{Output: &Output{Type: schemaType}, ExecuteStatementOutput: &dynamodb.ExecuteStatementOutput{}, Encoder: attributevalue.NewEncoder()}
}
