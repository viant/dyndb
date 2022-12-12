package dyndb

import (
	"context"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/francoispqt/gojay"
	ndynamodb "github.com/viant/dyndb/internal/dynamodb"
	"github.com/viant/dyndb/internal/exec"
	"io"
	"reflect"
)

//Rows represents rows driver
type Rows struct {
	execution    *exec.Execution
	client       *dynamodb.Client
	parameters   []types.AttributeValue
	deserializer *ndynamodb.DeserializeMiddleware
	columns      []string
	state        *exec.State
	index        int
	nextToken    *string
	ql           string
	limit        *int32
}

func (r *Rows) executeQueryStatement(ctx context.Context) error {
	_, err := r.client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  &r.ql,
		NextToken:  r.nextToken,
		Parameters: r.parameters,
		Limit:      r.limit,
	}, func(options *dynamodb.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			stack.Deserialize.Clear()
			return stack.Deserialize.Add(r.deserializer, middleware.After)
		})
	})
	if err != nil {
		return err
	}
	r.nextToken = r.deserializer.Output.NextToken
	if err != nil {
		return err
	}
	return nil

}

// Columns returns query columns
func (r *Rows) Columns() []string {
	if len(r.columns) > 0 {
		return r.columns
	}
	var columns []string
	for _, column := range r.state.Type.Columns {
		columns = append(columns, column.Name)
	}
	r.columns = columns
	return r.columns
}

// Close closes rows
func (r *Rows) Close() error {
	if r.state == nil {
		return nil
	}
	r.execution.ReleaseState(r.state)
	r.state = nil
	return nil
}

// Next moves to next row
func (r *Rows) Next(dest []driver.Value) error {
	if !r.hasNext() {
		if r.nextToken == nil {
			return io.EOF
		}
		if err := r.executeQueryStatement(context.Background()); err != nil {
			return err
		}
		if !r.hasNext() {
			return io.EOF
		}
	}
	output := r.deserializer.Output

	row := output.Rows[r.index]
	r.index++
	data := output.Data[row.Begin:row.End]

	r.state.SetDest(dest)
	err := gojay.Unmarshal(data, r.state)
	if err == nil {
		err = r.state.Reconcile()
	}
	return err
}

// hasNext returns true if there is next row to fetch.
func (r *Rows) hasNext() bool {
	if r.limit != nil {
		if withinLimit := r.index < int(*r.limit); !withinLimit {
			return withinLimit
		}
	}
	return r.index < len(r.deserializer.Output.Rows)
}

// ColumnTypeScanType returns column scan type
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	column := r.state.Type.Columns[index]
	return column.Type
}

// ColumnTypeDatabaseTypeName returns column database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	rType := r.ColumnTypeScanType(index)
	switch rType.Kind() {
	case reflect.Int:
		return "INT"
	case reflect.Float64:
		return "DECIMAL"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "STRING"
	case reflect.Slice:
		switch rType.Elem().Kind() {
		case reflect.Int:
			return "INTS"
		case reflect.Float64:
			return "DECIMALS"
		case reflect.String:
			return "STRINGS"
		case reflect.Uint8:
			return "BYTES"
		}
	}
	return ""
}

// ColumnTypeNullable returns if column is nullable
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return !r.deserializer.Output.Type.Fields[index].Required, true
}
