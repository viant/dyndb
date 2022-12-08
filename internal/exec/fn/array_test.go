package fn_test

import (
	"database/sql/driver"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dyndb/internal/exec"
	"github.com/viant/dyndb/internal/exec/fn"
	"github.com/viant/sqlparser"
	"reflect"
	"testing"
)

func TestNewArrayExists(t *testing.T) {

	var testCases = []struct {
		description string
		state       *exec.State
		initState   func(state *exec.State)
		value       interface{}
		expr        string
		expect      bool
	}{
		{
			description: "single element exists",
			expr:        "ARRAY_EXISTS(Collection, 'ElemX')",
			state:       exec.newState(exec.NewType(false), nil),
			initState: func(state *exec.State) {
				state.Type.Add("Collection", "Collection", "", false)
				state.Fields = []driver.Value{[]string{"Elem1", "Elem2", "ElemX", "ElemN"}}
			},
			expect: true,
		},
		{
			description: "single element not exists",
			expr:        "ARRAY_EXISTS(Collection, 'Elem2')",
			state:       exec.newState(exec.NewType(false), nil),
			initState: func(state *exec.State) {
				state.Type.Add("Collection", "Collection", "", false)
				state.Fields = []driver.Value{[]string{"Elem1", "ElemX", "ElemN"}}
			},
		},
		{
			description: "ints not exists",
			expr:        "ARRAY_EXISTS(Collection, 123)",
			state:       exec.newState(exec.NewType(false), nil),
			initState: func(state *exec.State) {
				state.Type.Add("Collection", "Collection", "", false)
				state.Fields = []driver.Value{[]int{1, 2, 3}}
			},
		},
		{
			description: "int  exists",
			expr:        "ARRAY_EXISTS(Collection, 4)",
			state:       exec.newState(exec.NewType(false), nil),
			initState: func(state *exec.State) {
				state.Type.Add("Collection", "Collection", "", false)
				state.Fields = []driver.Value{[]int{1, 2, 3, 4, 5}}
			},
		},
	}

	for _, testCase := range testCases {
		call, err := sqlparser.ParseCallExpr(testCase.expr)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		f, fType, err := fn.newArrayExists(call, testCase.state.Type)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		if !assert.True(t, fType == reflect.TypeOf(true)) {
			continue
		}
		if testCase.initState != nil {
			testCase.initState(testCase.state)
		}
		actual, err := f.Exec(testCase.value, testCase.state)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		assert.EqualValuesf(t, testCase.expect, actual, testCase.description)
	}
}
