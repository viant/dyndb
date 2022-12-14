package exec

import (
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/francoispqt/gojay"
	"reflect"
	"sync/atomic"
)

//State represent an execution state
type State struct {
	Type    *Type
	Fields  []driver.Value
	Columns []driver.Value
	Reset   []driver.Value
	Args    []driver.NamedValue
	fieldUnmarshaler
	initialised int32
}

//QueryParameters returns query params
func (s *State) QueryParameters() ([]types.AttributeValue, error) {
	var result []types.AttributeValue
	if len(s.Type.Criteria) == 0 {
		return result, nil
	}
	pos := 0
	for _, param := range s.Type.Criteria {
		if param.Kind == ParameterKindPlaceholder {
			arg := s.Args[pos].Value
			pos++
			attrValue, err := Encode(arg)
			if err != nil {
				return nil, fmt.Errorf("failed to encode args: %T(%v), %w", arg, arg, err)
			}
			result = append(result, attrValue)
		}
	}
	return result, nil
}

//ProjectionListParameters returns list attributes
func (s *State) ProjectionListParameters() ([]types.AttributeValue, error) {
	var result []types.AttributeValue
	if len(s.Type.List) == 0 {
		return result, nil
	}
	pos := 0

	for _, param := range s.Type.List {
		if param.Kind == ParameterKindPlaceholder {
			arg := s.Args[pos].Value
			pos++
			attrValue, err := Encode(arg)
			if err != nil {
				return nil, fmt.Errorf("failed to encode args: %T(%v), %w", arg, arg, err)
			}
			result = append(result, attrValue)
		}
	}
	return result, nil
}

//Values returns values
func (s *State) Values(value interface{}, parameters []*Parameter) ([]interface{}, error) {
	var result = make([]interface{}, len(parameters))
	for i, param := range parameters {
		switch param.Kind {
		case ParameterKindLiteral:
			result[i] = param.Value
		case ParameterKindColumn:
			result[i] = s.Columns[param.Pos]
		case ParameterKindValue:
			result[i] = value
		case ParameterKindField:
			result[i] = s.Fields[param.Pos]
		case ParameterKindPlaceholder:
			result[i] = s.Args[param.Pos]
		default:
			return nil, fmt.Errorf("unsupported parameter: %+v", param)
		}
	}
	return result, nil
}

//NKeys returns keys
func (s *State) NKeys() int {
	return 0
}

// UnmarshalJSONObject implements gojay's UnmarshalerJSONObject
func (s *State) UnmarshalJSONObject(dec *gojay.Decoder, k string) (err error) {
	pos, ok := s.Type.fields[k]
	if !ok {
		return fmt.Errorf("unknown field: %s", k)
	}
	s.fieldUnmarshaler.field = &s.Type.Fields[pos]
	return dec.Object(&s.fieldUnmarshaler)
}

//Init initialise state
func (s *State) Init() error {
	if !atomic.CompareAndSwapInt32(&s.initialised, 0, 1) {
		return nil
	}
	if err := s.Type.Init(); err != nil {
		return err
	}
	s.Reset = make([]driver.Value, len(s.Type.Fields))
	s.Fields = make([]driver.Value, len(s.Type.Fields))
	for i := range s.Type.Fields {
		if field := &s.Type.Fields[i]; field.IsSlice() {
			s.Reset[field.Pos] = reflect.MakeSlice(field.Type, 0, 0).Interface()
		}
	}
	return nil
}

//SetDest set destination values
func (s *State) SetDest(dest []driver.Value) {
	copy(s.Fields, s.Reset)
	s.Columns = dest
	s.fieldUnmarshaler.values = s.Fields
}

//Reconcile reconcile state
func (s *State) Reconcile() error {
	var err error
	for i := range s.Type.Columns {
		column := &s.Type.Columns[i]
		if column.Func == nil {
			field := &s.Type.Fields[column.Fields[0]]
			s.Columns[column.Pos] = s.Fields[field.Pos]
			if s.Columns[column.Pos] == nil {
				s.Columns[column.Pos] = column.DefaultValue
			}
			continue
		}
		if s.Columns[column.Pos], err = column.Func.Exec(nil, s); err != nil {
			return err
		}
	}
	return err
}

func NewState(sType *Type, args []driver.NamedValue) *State {
	return &State{
		Type: sType,
		Args: args,
	}
}
