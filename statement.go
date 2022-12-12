package dyndb

import (
	"context"
	"database/sql/driver"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ndynamodb "github.com/viant/dyndb/internal/dynamodb"
	"github.com/viant/dyndb/internal/exec"

	//load buildin functions
	_ "github.com/viant/dyndb/internal/exec/fn"
	"time"
)

var maxWaitTime = 30 * time.Second

//Statement abstraction implements database/sql driver.Statement interface
type Statement struct {
	token     *string
	execution *exec.Execution
	state     *exec.State
	client    *dynamodb.Client
}

//Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	named := asNamedValues(args)
	return s.ExecContext(context.Background(), named)
}

//ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	switch s.execution.Kind {
	case exec.KindCreateTable:
		return s.createTable(ctx)
	case exec.KindDropTable:
		return s.dropTable(ctx)
	}
	state := s.execution.NewState(args)
	s.state = state
	ql := s.execution.Parti.Query
	parameters, err := state.ProjectionListParameters()
	if err != nil {
		return nil, err
	}
	if err = s.exec(ctx, ql, parameters); err != nil {
		return nil, err
	}
	return &result{totalRows: 1}, err
}

func (s *Statement) createTable(ctx context.Context) (driver.Result, error) {
	if s.execution.HasTable && s.execution.Create.IfDoesExists {
		return &result{}, nil
	}
	input, err := s.execution.CreateTableInput()
	if err != nil {
		return nil, err
	}

	output, err := s.client.CreateTable(ctx, input)
	if output != nil {
		description := output.TableDescription
		startTime := time.Now()
		for time.Now().Sub(startTime) < maxWaitTime {
			time.Sleep(100 * time.Millisecond)
			if description, _ = tableDescription(ctx, s.client, s.execution.Table); description == nil || description.TableStatus != "CREATING" {
				break
			}
		}
	}
	return nil, err
}

func (s *Statement) dropTable(ctx context.Context) (driver.Result, error) {
	if !s.execution.HasTable && s.execution.Drop.IfExists {
		return &result{}, nil
	}
	input, err := s.execution.DeleteTableInput()
	if err != nil {
		return nil, err
	}
	if _, err = s.client.DeleteTable(ctx, input); err != nil {
		return nil, err
	}
	startTime := time.Now()
	for time.Now().Sub(startTime) < maxWaitTime {
		time.Sleep(100 * time.Millisecond)
		if description, _ := tableDescription(ctx, s.client, s.execution.Table); description == nil {
			break
		}
	}
	return nil, err
}

//Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	named := asNamedValues(args)
	return s.QueryContext(context.TODO(), named)
}

func asNamedValues(args []driver.Value) []driver.NamedValue {
	var named []driver.NamedValue
	for i := range args {
		named = append(named, driver.NamedValue{Ordinal: i, Value: args[i]})
	}
	return named
}

//QueryContext runs query
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	ql := s.execution.Parti.Query
	deserializer := ndynamodb.NewDeserializeMiddleware(s.execution.Type)
	state := s.execution.NewState(args)
	parameters, err := state.QueryParameters()
	if err != nil {
		return nil, err
	}
	rows := &Rows{client: s.client,
		state:        state,
		deserializer: deserializer,
		execution:    s.execution,
		ql:           ql,
		parameters:   parameters,
		limit:        s.execution.Limit,
	}

	if err := rows.executeQueryStatement(ctx); err != nil {
		return nil, err
	}
	err = state.Init()
	return rows, err
}

func (s *Statement) exec(ctx context.Context, query string, parameters []types.AttributeValue) error {
	_, err := s.client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
		Statement:  &query,
		Parameters: parameters,
	})
	return err
}

//CheckNamedValue checks supported types (all for now)
func (s *Statement) CheckNamedValue(named *driver.NamedValue) error {
	return nil
}

//NumInput returns numinput
func (s *Statement) NumInput() int {
	return s.execution.Type.NumInput()
}

//Close closes statement
func (s *Statement) Close() error {
	if s.state == nil {
		return nil
	}
	s.execution.ReleaseState(s.state)
	s.state = nil
	return nil
}
