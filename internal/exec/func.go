package exec

import (
	"github.com/viant/sqlparser/expr"
	"reflect"
)

//NewFunc function provider
type NewFunc func(call *expr.Call, rowType *Type) (Function, reflect.Type, error)

//Function user defined function interface
type Function interface {
	//Exec runs function logic
	Exec(value interface{}, state *State) (interface{}, error)
}
