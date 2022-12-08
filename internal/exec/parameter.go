package exec

import (
	"reflect"
	"strconv"
)

const (
	//ParameterKindLiteral literal
	ParameterKindLiteral = ParameterKind(1)
	//ParameterKindPlaceholder placeholder
	ParameterKindPlaceholder = ParameterKind(2)
	//ParameterKindField field
	ParameterKindField = ParameterKind(3)
	//ParameterKindColumn column
	ParameterKindColumn = ParameterKind(4)
	//ParameterKindValue values
	ParameterKindValue = ParameterKind(5)
)

type (
	//ParameterKind defines parameter king
	ParameterKind string
	//Parameter represents query parameters
	Parameter struct {
		Name  string
		Type  reflect.Type
		Kind  ParameterKind
		Pos   int
		Value interface{} //for constants
	}
	//Parameters parameter collections
	Parameters struct {
		BindingLen int
		List       []*Parameter
		Criteria   []*Parameter
		numInput   int
	}
)

//AddCriteria add criteria paramaeter
func (p *Parameters) AddCriteria(param *Parameter) {
	p.initParam(param)
	if param.Kind == ParameterKindPlaceholder {
		p.numInput++
	}
	p.Criteria = append(p.Criteria, param)

}

//AddItem add param
func (p *Parameters) AddItem(param *Parameter) {
	p.initParam(param)
	p.List = append(p.List, param)
}

func (p *Parameters) initParam(param *Parameter) {
	if param.Kind == ParameterKindPlaceholder {
		param.Pos = p.BindingLen
		p.BindingLen++
	}
}

//NumInput returns num inputs
func (p *Parameters) NumInput() int {
	return p.numInput
}

//NewLiteral returns a literal param
func NewLiteral(text string, kind string) *Parameter {
	var value interface{}
	switch kind {
	case "string":
		value = text[1 : len(text)-1] //trunc quotes
	case "int":
		value, _ = strconv.Atoi(text)
	case "numeric":
		value, _ = strconv.ParseFloat(text, 64)
	case "bool":
		value, _ = strconv.ParseBool(text)
	default:
		value = text
	}
	return &Parameter{
		Name:  "",
		Type:  reflect.TypeOf(value),
		Kind:  ParameterKindLiteral,
		Value: value,
	}
}

//NewPlaceholder returns a placeholder param
func NewPlaceholder(name string) *Parameter {
	return &Parameter{
		Name: name,
		Kind: ParameterKindPlaceholder,
	}
}

//NewField returns a field param
func NewField(name string) *Parameter {
	return &Parameter{
		Name: name,
		Kind: ParameterKindField,
	}
}

//NewColumn returns a column param
func NewColumn(name string, pos int) *Parameter {
	return &Parameter{
		Name: name,
		Pos:  pos,
		Kind: ParameterKindColumn,
	}
}

//NewValue returns a value param
func NewValue() *Parameter {
	return &Parameter{
		Kind: ParameterKindValue,
	}
}
