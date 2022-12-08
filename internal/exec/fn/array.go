package fn

import (
	"fmt"
	"github.com/viant/dyndb/internal/exec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type arrayExists struct {
	params     []*exec.Parameter
	prev       exec.Function
	xSlice     *xunsafe.Slice
	xItemType  *xunsafe.Type
	shallDeref bool
}

func (a *arrayExists) Exec(value interface{}, state *exec.State) (interface{}, error) {
	var err error
	if a.prev != nil {
		if value, err = a.prev.Exec(value, state); err != nil {
			return nil, err
		}
	}
	elements, err := state.Values(value, a.params)
	if err != nil {
		return false, err
	}
	value = elements[0]

	if candidates, ok := value.([]interface{}); ok {
		return a.hasMatch(candidates, elements), nil
	}

	if err = a.init(value); err != nil {
		return nil, err
	}
	ptr := xunsafe.AsPointer(value)
	exists := a.elementExists(ptr, elements[1:])
	return exists, nil
}

func (a *arrayExists) hasMatch(candidates []interface{}, elements []interface{}) bool {
	for _, candidate := range candidates {
		for _, elem := range elements[1:] {
			if elem == candidate {
				return true
			}
		}
	}
	return false
}

func (a *arrayExists) init(value interface{}) error {
	if a.xSlice != nil {
		return nil
	}
	rValue := reflect.TypeOf(value)
	if rValue.Kind() == reflect.Ptr {
		rValue = rValue.Elem()
	}
	if rValue.Kind() != reflect.Slice {
		return fmt.Errorf("expected array but had: %T", value)
	}
	a.xSlice = xunsafe.NewSlice(rValue)
	a.shallDeref = rValue.Elem().Kind() != reflect.Ptr
	if a.shallDeref {
		a.xItemType = xunsafe.NewType(rValue.Elem())
	}
	return nil
}

func (a *arrayExists) elementExists(ptr unsafe.Pointer, elements []interface{}) bool {
	sliceLen := a.xSlice.Len(ptr)
	if sliceLen == 0 {
		return false
	}
	switch len(elements) {
	case 0:
		return false
	case 1:
		for i := 0; i < sliceLen; i++ {
			candidate := a.sliceItem(ptr, i)
			if candidate == elements[0] {
				return true
			}
		}
		return false
	case 2:
		for i := 0; i < sliceLen; i++ {
			candidate := a.sliceItem(ptr, i)
			if candidate == elements[0] || candidate == elements[1] {
				return true
			}
		}
		return false
	case 3:
		for i := 0; i < sliceLen; i++ {
			candidate := a.sliceItem(ptr, i)
			if candidate == elements[0] || candidate == elements[1] || candidate == elements[2] {
				return true
			}
		}
		return false
	case 4:
		for i := 0; i < sliceLen; i++ {
			candidate := a.sliceItem(ptr, i)
			if candidate == elements[0] || candidate == elements[1] || candidate == elements[2] || candidate == elements[3] {
				return true
			}
		}
		return false
	}
	for _, element := range elements {
		for i := 0; i < sliceLen; i++ {
			candidate := a.sliceItem(ptr, i)
			if candidate == element {
				return true
			}
		}
	}
	return false
}

func (a *arrayExists) addParameter(param *exec.Parameter) {
	a.params = append(a.params, param)
}

func (a *arrayExists) sliceItem(ptr unsafe.Pointer, i int) interface{} {
	candidate := a.xSlice.ValuePointerAt(ptr, i)
	if a.shallDeref {
		candidate = a.xItemType.Interface(xunsafe.AsPointer(candidate))
	}
	return candidate
}

func NewArrayExists(call *expr.Call, rowType *exec.Type) (exec.Function, reflect.Type, error) {
	fn, err := newArrayExistsFn(call, rowType)
	if err != nil {
		return nil, nil, err
	}
	return fn, reflect.TypeOf(true), nil
}

func newArrayExistsFn(call *expr.Call, rowType *exec.Type) (exec.Function, error) {
	result := &arrayExists{}
	if len(call.Args) < 2 {
		return nil, fmt.Errorf("invalid argument count, expected 2 but had: %v", len(call.Args))
	}
	fieldIdentifier := expr.Identity(call.Args[0])
	if fieldIdentifier == nil {
		return nil, fmt.Errorf("unsupported node ARRAY_EXISTS(%T)", call.Args[0])
	}
	fieldName := sqlparser.Stringify(fieldIdentifier)
	field := rowType.Field(fieldName)

	fieldParam := exec.NewField(fieldName)
	fieldParam.Pos = field.Pos
	result.addParameter(fieldParam)
	for i := 1; i < len(call.Args); i++ {
		switch actual := call.Args[i].(type) {
		case *expr.Placeholder:
			parameter := exec.NewPlaceholder("")
			rowType.AddItem(parameter)
			result.addParameter(parameter)
		case *expr.Literal:
			parameter := exec.NewLiteral(actual.Value, actual.Kind)
			result.addParameter(parameter)
		default:
			return nil, fmt.Errorf("unsupported node ARRAY_EXISTS(Ident, %T)", actual)
		}
	}
	return result, nil
}
