package exec

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/viant/sqlparser"
	del "github.com/viant/sqlparser/delete"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/insert"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlparser/table"
	"github.com/viant/sqlparser/update"
	"strconv"
	"strings"
	"sync"
)

//Kind represents execution king
type Kind int

const (
	//KindUndefined undefined kind
	KindUndefined = Kind(iota)
	//KindCreateTable create table
	KindCreateTable
	//KindDropTable drop table
	KindDropTable
)

type (
	//Execution represent execution
	Execution struct {
		Kind          Kind
		SQL           string
		Table         string
		HasTable      bool
		query         *query.Select
		insert        *insert.Statement
		update        *update.Statement
		delete        *del.Statement
		Create        *table.Create
		Drop          *table.Drop
		Type          *Type
		Parti         *PartiQL
		Limit         *int32
		state         sync.Pool
		criteriaParam string
	}

	//PartiQL represent PrtiQA
	PartiQL struct {
		Query string
	}
)

//ReleaseState releases state
func (e *Execution) ReleaseState(state *State) {
	e.state.Put(state)
}

func (e *Execution) initCriteria() error {
	var qualifies []*expr.Qualify

	if e.query.IsNested() {
		nested := e.query.NestedSelect()
		if nested.Qualify != nil {
			qualifies = append(qualifies, nested.Qualify)
		}
	}
	if aQuery := e.query; aQuery.Qualify != nil {
		qualifies = append(qualifies, aQuery.Qualify)
	}
	if len(qualifies) == 0 {
		return nil
	}
	for _, qualify := range qualifies {
		err := e.parseCriteria(qualify.X)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Execution) parseCriteria(x node.Node) error {
	if x == nil {
		return nil
	}
	var paramName string
	var ok bool
	var err error
	switch actual := x.(type) {
	case *expr.Ident, *expr.Selector:
		e.criteriaParam = sqlparser.Stringify(actual)
	case *expr.Placeholder:
		e.Type.AddCriteria(NewPlaceholder(e.criteriaParam))
	case *expr.Parenthesis:
		return e.parseCriteria(actual.X)
	case *expr.Binary:
		paramName, ok, err = e.appendPlaceholder(actual.X, actual.Y)
		if ok || err != nil {
			return err
		}
		paramName, ok, err = e.appendPlaceholder(actual.Y, actual.X)
		if ok || err != nil {
			return err
		}
		switch strings.ToLower(actual.Op) {
		case "in":
			var nodeToCheck node.Node
			group := actual.Parenthesis()
			if group == nil {
				if yOp, ok := actual.Y.(*expr.Binary); ok {
					if group = yOp.Parenthesis(); group != nil {
						nodeToCheck = yOp.Y
					}
				}
			}
			list, err := sqlparser.ParseList(strings.Trim(group.Raw, "()"))
			if err != nil {
				return err
			}
			for _, item := range list {
				if _, ok := item.Expr.(*expr.Placeholder); ok {
					e.Type.AddCriteria(NewPlaceholder(paramName))
				}
			}
			if nodeToCheck != nil {
				return e.parseCriteria(nodeToCheck)
			}
			return nil
		}
		if err = e.parseCriteria(actual.X); err != nil {
			return err
		}
		return e.parseCriteria(actual.Y)
	case *expr.Literal:
	default:
		panic(fmt.Sprintf("%T: unupported", actual))
	}
	return nil
}

func (e *Execution) appendPlaceholder(x, y node.Node) (string, bool, error) {
	paramName := ""
	if ident := expr.Identity(x); ident != nil {
		paramName = sqlparser.Stringify(ident)
		if _, ok := y.(*expr.Placeholder); ok {
			e.Type.AddCriteria(NewPlaceholder(paramName))
			return paramName, true, nil
		}
	}
	return paramName, false, nil
}
func (e *Execution) buildQuery() error {
	e.Parti = &PartiQL{}

	query := e.query
	if query.IsNested() {
		query = query.NestedSelect()
	}
	builder := new(bytes.Buffer)
	builder.WriteString("SELECT ")
	if e.Type.Wildcard {
		builder.WriteString("*")
	} else {
		i := 0
		for _, field := range e.Type.Fields {
			if i > 0 {
				builder.WriteString(", ")
			}
			i++
			builder.WriteString(field.Name)
		}
	}
	builder.WriteString(" FROM ")
	builder.WriteString(e.Table)

	var qualifies []string
	if query.Qualify != nil {
		qualifies = append(qualifies, sqlparser.Stringify(query.Qualify.X))
	}

	if query != e.query && e.query.Qualify != nil {
		qualifies = append(qualifies, sqlparser.Stringify(e.query.Qualify))
	}
	if len(qualifies) > 0 {
		builder.WriteString(" WHERE ")
		if len(qualifies) > 1 {
			qualifies[0] = "(" + qualifies[0] + ")"
		}
		builder.WriteString(strings.Join(qualifies, " AND "))
	}

	if len(e.query.OrderBy) > 0 {
		builder.WriteString(" ORDER BY ")
		builder.WriteString(sqlparser.Stringify(e.query.OrderBy))
	}
	e.Parti = &PartiQL{
		Query: builder.String(),
	}
	return nil
}

func (e *Execution) initQuery(desc *types.TableDescription) error {

	aQuery := e.query
	var outerColumns = Columns{}
	if aQuery.IsNested() {
		if err := outerColumns.init(aQuery); err != nil {
			return err
		}
		aQuery = aQuery.NestedSelect()
	}
	rowType := NewType(aQuery.List.IsStarExpr())
	if err := e.adjustQueryType(desc, rowType, aQuery, outerColumns); err != nil {
		return err
	}
	e.Type = rowType
	e.initState()
	return e.buildQuery()
}

func (e *Execution) adjustQueryType(desc *types.TableDescription, rowType *Type, query *query.Select, oCcolumns Columns) error {
	outerColumns := oCcolumns.index()
	for _, key := range desc.KeySchema {
		rowType.Keys[*key.AttributeName] = key.KeyType
	}
	if rowType.Wildcard {
		e.buildWildcardType(desc, rowType)
		return nil
	}
	attrTypes := buildAttributeTypes(desc)
	for _, item := range query.List {
		switch actual := item.Expr.(type) {
		case *expr.Ident:
			attrType, isRequired := attrTypes[actual.Name]
			cName := item.Alias
			if cName == "" {
				cName = actual.Name
			}
			if !outerColumns.ShallOutput(cName) {
				continue
			}
			_, column := rowType.Add(actual.Name, item.Alias, attrType, isRequired)
			if outer, ok := outerColumns[cName]; ok {
				column.DefaultValue = outer.DefaultValue
			}

		case *expr.Selector:
			name := sqlparser.Stringify(actual)
			attrType, isRequired := attrTypes[name]
			cName := item.Alias
			if cName == "" {
				cName = name
			}
			if !outerColumns.ShallOutput(cName) {
				continue
			}
			_, column := rowType.Add(name, item.Alias, attrType, isRequired)
			if outer, ok := outerColumns[cName]; ok {
				column.DefaultValue = outer.DefaultValue
			}
		case *expr.Call:
			fName := strings.ToLower(sqlparser.Stringify(actual.X))
			if attrType, ok := attributeTypeCast[fName]; ok {
				name := sqlparser.Stringify(actual.Args[0])
				rowType.Add(name, item.Alias, attrType, false)
				continue
			}
			newFunc := funcRegistry.Lookup(fName)
			if newFunc == nil {
				return fmt.Errorf("unknown function: %v", fName)
			}
			fn, fnType, err := newFunc(actual, rowType)
			if err != nil {
				return err
			}
			column := rowType.Column(item.Alias)
			column.Type = fnType
			column.Func = fn
		default:
			return fmt.Errorf("unsupported projection node %T", actual)
		}
	}
	return nil
}

func buildAttributeTypes(desc *types.TableDescription) map[string]string {
	attrTypes := map[string]string{}
	for _, attr := range desc.AttributeDefinitions {
		attrTypes[*attr.AttributeName] = string(attr.AttributeType)
	}
	return attrTypes
}

func (e *Execution) buildWildcardType(desc *types.TableDescription, sType *Type) {
	for _, attr := range desc.AttributeDefinitions {
		sType.Add(*attr.AttributeName, "", string(attr.AttributeType), true)
	}
}

func (e *Execution) initInsert(desc *types.TableDescription) error {
	rowType := NewType(false)
	attrTypes := buildAttributeTypes(desc)
	e.Parti = &PartiQL{}
	builder := strings.Builder{}
	builder.WriteString("INSERT INTO ")
	builder.WriteString(*desc.TableName)
	builder.WriteString(" VALUE {")
	for i, column := range e.insert.Columns {
		if i > 0 {
			builder.WriteString(",")
		}
		delete(attrTypes, column)
		builder.WriteString("'")
		builder.WriteString(column)
		builder.WriteString("':")
		switch actual := e.insert.Values[i].Expr.(type) {
		case *expr.Placeholder:
			rowType.AddItem(NewPlaceholder(column))
			rowType.numInput++
			builder.WriteString("?")
		case *expr.Literal:
			builder.WriteString(actual.Value)
		case *expr.Call:
			fName := strings.ToLower(sqlparser.Stringify(actual.X))
			switch fName {
			case "strings", "array", "ints", "decimals":
				builder.WriteString("<<")
				for j, arg := range actual.Args {
					if j > 0 {
						builder.WriteString(",")
					}
					builder.WriteString(sqlparser.Stringify(arg))
				}
				builder.WriteString(">>")
				continue
			case "list":
				builder.WriteString("[")
				for j, arg := range actual.Args {
					if j > 0 {
						builder.WriteString(",")
					}
					builder.WriteString(sqlparser.Stringify(arg))
				}
				builder.WriteString("]")
				continue
			case "map":
				args := actual.Raw[1 : len(actual.Raw)-1]
				builder.WriteString(args)
				continue
			case "t":
				args := actual.Raw[1 : len(actual.Raw)-1]
				builder.WriteString(args)
				continue
			}
			return fmt.Errorf("not supported: %T", actual)

		default:
			return fmt.Errorf("not supported: %T", actual)
		}
	}
	if len(attrTypes) > 0 {
		for k := range attrTypes {
			return fmt.Errorf(k + " is required")
		}
	}
	builder.WriteString("}")
	e.Parti = &PartiQL{Query: builder.String()}
	e.Type = rowType
	e.initState()
	return nil
}

func (e *Execution) initUpdate(desc *types.TableDescription) error {
	if e.query.Qualify == nil {
		return fmt.Errorf("where clause is required")
	}
	rowType := NewType(false)
	e.Parti = &PartiQL{}
	builder := strings.Builder{}
	builder.WriteString("UPDATE ")
	builder.WriteString(*desc.TableName)
	builder.WriteString("\n")
	for i, column := range e.insert.Columns {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("SET ")
		builder.WriteString(column)
		builder.WriteString("=")
		switch actual := e.insert.Values[i].Expr.(type) {
		case *expr.Placeholder:
			rowType.AddItem(NewPlaceholder(column))
			rowType.numInput++
			builder.WriteString("?")
		case *expr.Literal:
			builder.WriteString(actual.Value)
		}
	}
	builder.WriteString(" WHERE ")
	builder.WriteString(sqlparser.Stringify(e.query.Qualify.X))
	if err := e.initCriteria(); err != nil {
		return err
	}
	e.Parti = &PartiQL{Query: builder.String()}
	e.Type = rowType
	e.initState()
	return nil
}

func (e *Execution) initDelete(desc *types.TableDescription) error {
	if e.query.Qualify == nil {
		return fmt.Errorf("where clause is required")
	}
	rowType := NewType(false)
	e.Parti = &PartiQL{}
	builder := strings.Builder{}
	builder.WriteString("DELETE FROM ")
	builder.WriteString(*desc.TableName)
	builder.WriteString("\n")
	builder.WriteString(" WHERE ")
	builder.WriteString(sqlparser.Stringify(e.query.Qualify.X))
	if err := e.initCriteria(); err != nil {
		return err
	}
	e.Parti = &PartiQL{Query: builder.String()}
	e.Type = rowType
	e.initState()
	return nil
}

//CreateTableInput returns create table input
func (e *Execution) CreateTableInput() (*dynamodb.CreateTableInput, error) {
	capacityUnits := int64(1)
	input := &dynamodb.CreateTableInput{
		TableName: &e.Create.Name,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  &capacityUnits,
			WriteCapacityUnits: &capacityUnits,
		},
	}
	for _, column := range e.Create.Columns {
		attrType, err := databaseAttributeType(column.Type)
		if err != nil {
			return nil, err
		}
		input.AttributeDefinitions = append(input.AttributeDefinitions, types.AttributeDefinition{
			AttributeName: &column.Name,
			AttributeType: types.ScalarAttributeType(attrType),
		})
		if key := column.Key; key != "" {
			key = strings.ToUpper(key)
			key = strings.TrimSpace(strings.Replace(key, "KEY", "", 1))
			input.KeySchema = append(input.KeySchema, types.KeySchemaElement{
				AttributeName: &column.Name,
				KeyType:       types.KeyType(key),
			})
		}
	}
	return input, nil
}

//DeleteTableInput returns delete table input
func (e *Execution) DeleteTableInput() (*dynamodb.DeleteTableInput, error) {
	return &dynamodb.DeleteTableInput{
		TableName: &e.Drop.Name,
	}, nil
}

//NewState ctates a state
func (e *Execution) NewState(args []driver.NamedValue) *State {
	state := e.state.Get()
	result := state.(*State)
	result.Args = args
	return result
}

//NewQuery creates an query execution
func NewQuery(table string, query *query.Select, desc *types.TableDescription) (*Execution, error) {
	result := &Execution{
		Table: table,
		query: query,
	}
	if limit := query.Limit; limit != nil {
		value, _ := strconv.Atoi(limit.Value)
		limit := int32(value)
		result.Limit = &limit
	}
	if err := result.initQuery(desc); err != nil {
		return nil, err
	}
	if err := result.initCriteria(); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Execution) initState() {
	e.state.New = func() interface{} {
		return NewState(e.Type, nil)
	}
}

//NewInsert creates an insert execution
func NewInsert(table string, stmt *insert.Statement, desc *types.TableDescription) (*Execution, error) {
	result := &Execution{
		Table:  table,
		insert: stmt,
	}
	if err := result.initInsert(desc); err != nil {
		return nil, err
	}
	return result, nil
}

//NewUpdate creates an update execution
func NewUpdate(table string, stmt *insert.Statement, desc *types.TableDescription) (*Execution, error) {
	result := &Execution{
		Table:  table,
		insert: stmt,
	}
	if err := result.initUpdate(desc); err != nil {
		return nil, err
	}

	return result, nil
}

//NewDelete creates an update execution
func NewDelete(table string, stmt *del.Statement, desc *types.TableDescription) (*Execution, error) {
	result := &Execution{
		Table:  table,
		delete: stmt,
	}
	if err := result.initDelete(desc); err != nil {
		return nil, err
	}
	return result, nil
}

//NewCreateTable returns create Table execution
func NewCreateTable(table string, stmt *table.Create, desc *types.TableDescription) (*Execution, error) {
	result := &Execution{
		Kind:     KindCreateTable,
		Table:    table,
		Create:   stmt,
		Type:     NewType(false),
		HasTable: desc != nil,
	}
	return result, nil
}

//NewDropTable returns drop Table execution
func NewDropTable(table string, stmt *table.Drop, desc *types.TableDescription) (*Execution, error) {
	result := &Execution{
		Kind:     KindDropTable,
		Table:    table,
		Drop:     stmt,
		Type:     NewType(false),
		HasTable: desc != nil,
	}

	return result, nil
}
