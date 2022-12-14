package exec

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/francoispqt/gojay"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"reflect"
	"strings"
	"sync/atomic"
)

type (
	//Type represent execution tpye
	Type struct {
		initialized int32
		Parameters
		Wildcard bool
		Fields   []Field
		fields   map[string]int
		columns  map[string]int
		Columns  []Column
		Keys     map[string]types.KeyType
	}

	//Field represents underlying storage field
	Field struct {
		Pos      int
		Name     string
		Type     reflect.Type
		linked   []int
		Required bool
		Decoder  func(dec *gojay.Decoder) (interface{}, error)
	}

	//Column represents projection column
	Column struct {
		Pos          int
		Name         string
		DefaultValue interface{}
		Fields       []int
		FieldNames   []string

		Type reflect.Type
		Func Function
	}

	Columns     []*Column
	IndexColumn map[string]*Column
)

func (i IndexColumn) ShallOutput(name string) bool {
	if len(i) == 0 {
		return true
	}
	_, ok := i[name]
	return ok
}

func (o *Columns) index() IndexColumn {
	var result = make(map[string]*Column)
	for i := range *o {
		result[(*o)[i].Name] = (*o)[i]
	}
	return result
}

func (o *Columns) init(aQuery *query.Select) error {
	for _, item := range aQuery.List {
		column := &Column{}
		if item.Alias != "" {
			column.Name = item.Alias
		}
		switch actual := item.Expr.(type) {
		case *expr.Call:
			if fName := sqlparser.Stringify(actual.X); strings.ToLower(fName) == "coalesce" {
				if len(actual.Args) != 2 {
					return fmt.Errorf("coalesce invalid argsument count")
				}
				column.FieldNames = append(column.FieldNames, sqlparser.Stringify(actual.Args[0]))
				if literal, ok := actual.Args[1].(*expr.Literal); ok {
					param := NewLiteral(literal.Value, literal.Kind)
					column.DefaultValue = param.Value
				}
			}
		case *expr.Star:
		case *expr.Ident, *expr.Selector:
			if column.Name == "" {
				column.Name = sqlparser.Stringify(actual)
			}
		}
	}
	return nil
}

//Column returns a column
func (t *Type) Column(name string) *Column {
	pos, ok := t.columns[name]
	if ok {
		return &t.Columns[pos]
	}
	pos = len(t.columns)
	t.Columns = append(t.Columns, Column{Name: name, Pos: pos})
	t.columns[name] = pos
	return &t.Columns[pos]
}

//Add adds field and column and link
func (t *Type) Add(fName, cName, attrType string, isRequired bool) (*Field, *Column) {
	field := t.Field(fName)
	field.Required = isRequired
	field.Type = Convert(attrType)
	if cName == "" {
		cName = fName
	}
	column := t.Column(cName)
	column.Link(field)
	return field, column
}

func normalizeKey(name string) string {
	if index := strings.LastIndexByte(name, '.'); index != -1 {
		return name[index+1:]
	}
	return name
}

//Field returns a filed
func (t *Type) Field(name string) *Field {
	key := normalizeKey(name)
	pos, ok := t.fields[key]
	if ok {
		return &t.Fields[pos]
	}
	pos = len(t.Fields)
	t.Fields = append(t.Fields, Field{Pos: pos, Name: name})
	t.fields[key] = pos
	return &t.Fields[pos]
}

//Link links field with column
func (c *Column) Link(field *Field) {
	c.Fields = append(c.Fields, field.Pos)
	if c.Type == nil {
		c.Type = field.Type
	}
	field.linked = append(field.linked, c.Pos)
}

//AddCriteria adds criteria
func (t *Type) AddCriteria(param *Parameter) {
	t.Parameters.AddCriteria(param)
	t.updatePos(param)
}

//AddItem add param
func (t *Type) AddItem(param *Parameter) {
	t.Parameters.AddItem(param)
	t.updatePos(param)
}

func (t *Type) updatePos(parameter *Parameter) {
	switch parameter.Kind {
	case ParameterKindColumn:
		column := t.Column(parameter.Name)
		parameter.Pos = column.Pos
	case ParameterKindField:
		field := t.Field(parameter.Name)
		parameter.Pos = field.Pos
	}
}

//Init initialise type
func (t *Type) Init() error {
	if !atomic.CompareAndSwapInt32(&t.initialized, 0, 1) {
		return nil
	}
	t.ensureTypes()
	return t.ensureDecoders()
}

func (t *Type) ensureTypes() {
	for i := range t.Fields {
		field := &t.Fields[i]
		if field.Type == nil {
			field.Type = stringType
		}
		for _, linkPos := range field.linked {
			column := &t.Columns[linkPos]
			if column.Type == nil {
				column.Type = field.Type
			}
		}
	}
}

func (t *Type) ensureDecoders() error {
	var err error
	for i := range t.Fields {
		field := &t.Fields[i]
		if field.Decoder != nil {
			continue
		}
		if field.Decoder, err = decoderFor(field.Type, field.Required); err != nil {
			return err
		}
	}
	return nil
}

//IsSlice returns true if a filed is slice
func (f *Field) IsSlice() bool {
	return f.Type.Kind() == reflect.Slice
}

//NewType creates a new type
func NewType(wildcard bool) *Type {
	return &Type{
		fields:   make(map[string]int, 10),
		columns:  make(map[string]int, 10),
		Wildcard: wildcard, Keys: map[string]types.KeyType{}}
}
