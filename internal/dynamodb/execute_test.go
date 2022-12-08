package dynamodb

import (
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dyndb/internal/exec"
	"testing"
)

func TestExecuteStatementOutput_UnmarshalJSONObject(t *testing.T) {

	var testCases = []struct {
		description string
		input       string
		expect      []string
	}{
		{
			description: "array regions",
			input: `{"Items":[
						{"UserId":{"N":"2"},"Name":{"S":"User 2"}},
						{"UserId":{"N":"1"}},
						{"UserId":{"N":"1"},"Name":{"S":"User 1"}}]}`,
			expect: []string{
				`{"UserId":{"N":"2"},"Name":{"S":"User 2"}}`,
				`{"UserId":{"N":"1"}}`,
				`{"UserId":{"N":"1"},"Name":{"S":"User 1"}}`,
			},
		},
	}

	for _, testCase := range testCases {
		target := NewExecuteStatementOutput(exec.NewType(true))
		target.Data = []byte(testCase.input)
		err := gojay.Unmarshal(target.Data, target)
		assert.Nilf(t, err, testCase.description)
		var actual []string
		for _, region := range target.Rows {
			actual = append(actual, string(target.Data[region.Begin:region.End]))
		}
		assert.EqualValuesf(t, testCase.expect, actual, testCase.description)
	}

}
