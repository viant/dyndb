package dyndb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/viant/dyndb/internal/exec"
	"github.com/viant/sqlparser"
	"strings"
)

//Connection represent connection
type Connection struct {
	cfg    *aws.Config
	client *dynamodb.Client
	executions
}

// Prepare returns a prepared statement, bound to this Connection.
func (c *Connection) Prepare(SQL string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), SQL)
}

// PrepareContext returns a prepared statement, bound to this Connection.
func (c *Connection) PrepareContext(ctx context.Context, SQL string) (driver.Stmt, error) {
	execution, err := c.getExecution(ctx, SQL)
	if err != nil {
		return nil, err
	}

	return &Statement{execution: execution, client: c.client}, err
}

func sqlLowerPrefix(SQL string) string {
	SQLPrefix := strings.TrimSpace(SQL)
	if len(SQLPrefix) > 10 {
		SQLPrefix = SQLPrefix[:10]
	}
	return strings.ToLower(SQLPrefix)
}

func (c *Connection) queryExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	aQuery, err := sqlparser.ParseQuery(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(aQuery)
	desc, err := tableDescription(ctx, c.client, tableName)
	if err != nil {
		return nil, err
	}
	return exec.NewQuery(tableName, aQuery, desc)
}

func (c *Connection) insertExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	stmt, err := sqlparser.ParseInsert(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(stmt)
	desc, err := tableDescription(ctx, c.client, tableName)
	if err != nil {
		return nil, err
	}
	return exec.NewInsert(tableName, stmt, desc)
}

func (c *Connection) updateExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	stmt, err := sqlparser.ParseInsert(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(stmt)
	desc, err := tableDescription(ctx, c.client, tableName)
	if err != nil {
		return nil, err
	}
	return exec.NewUpdate(tableName, stmt, desc)
}

func (c *Connection) deleteExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	stmt, err := sqlparser.ParseDelete(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(stmt)
	desc, _ := tableDescription(ctx, c.client, tableName)
	return exec.NewDelete(tableName, stmt, desc)
}

func (c *Connection) createTableExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	spec, err := sqlparser.ParseCreateTable(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(spec)
	desc, _ := tableDescription(ctx, c.client, tableName)
	return exec.NewCreateTable(tableName, spec, desc)
}

func (c *Connection) dropTableExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	spec, err := sqlparser.ParseDropTable(SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	tableName := sqlparser.TableName(spec)
	desc, _ := tableDescription(ctx, c.client, tableName)
	return exec.NewDropTable(tableName, spec, desc)
}

func tableDescription(ctx context.Context, client *dynamodb.Client, table string) (*types.TableDescription, error) {
	var desc *types.TableDescription
	describeOutput, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
	if describeOutput != nil {
		desc = describeOutput.Table
	}
	return desc, err
}

//Ping pings server
func (c *Connection) Ping(ctx context.Context) error {
	return nil
}

// Begin starts and returns a new transaction.
func (c *Connection) Begin() (driver.Tx, error) {
	return &tx{c}, nil
}

// BeginTx starts and returns a new transaction.
func (c *Connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &tx{c}, nil
}

// Close closes Connection
func (c *Connection) Close() error {
	return nil
}

//ResetSession resets session
func (c *Connection) ResetSession(ctx context.Context) error {
	return nil
}

func (c *Connection) getExecution(ctx context.Context, SQL string) (*exec.Execution, error) {
	execution := c.executions.Lookup(SQL)
	if execution != nil {
		return execution, nil
	}
	var err error
	SQLType := sqlLowerPrefix(SQL)
	if strings.HasPrefix(SQLType, "select") {
		execution, err = c.queryExecution(ctx, SQL)
	} else if strings.HasPrefix(SQLType, "insert") {
		return c.insertExecution(ctx, SQL)
	} else if strings.HasPrefix(SQLType, "update") {
		return c.updateExecution(ctx, SQL)
	} else if strings.HasPrefix(SQLType, "delete") {
		return c.deleteExecution(ctx, SQL)
	} else if strings.HasPrefix(SQLType, "create") {
		return c.createTableExecution(ctx, SQL)
	} else if strings.HasPrefix(SQLType, "drop") {
		return c.dropTableExecution(ctx, SQL)
	} else {
		return nil, fmt.Errorf("unuspported query: %v", SQL)
	}
	if err != nil {
		return nil, err
	}
	execution.SQL = SQL
	c.executions.Put(execution)
	return execution, err
}

//IsValid check is Connection is valid
func (c *Connection) IsValid() bool {
	return true
}
