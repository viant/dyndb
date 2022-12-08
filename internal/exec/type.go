package exec

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/francoispqt/gojay"
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
		Pos  int
		Name string

		Type     reflect.Type
		linked   []int
		Required bool
		Decoder  func(dec *gojay.Decoder) (interface{}, error)
	}

	//Column represents projection column
	Column struct {
		Pos    int
		Name   string
		Fields []int
		Type   reflect.Type
		Func   Function
	}
)

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
func (t *Type) Add(fName, cName, attrType string, isRequired bool) *Field {
	field := t.Field(fName)
	field.Required = isRequired
	field.Type = Convert(attrType)
	if cName == "" {
		cName = fName
	}
	column := t.Column(cName)
	column.Link(field)
	return field
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
