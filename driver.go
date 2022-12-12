package dyndb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/viant/scy/auth/aws"
)

const (
	scheme = "dynamodb"
)

func init() {
	sql.Register(scheme, &Driver{})
}

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type Driver struct{}

// Open new Connection.
// See https://github.com/viant/dynamodb#dsn-data-source-name for how
// the DSN string is formatted
func (d Driver) Open(dsn string) (driver.Conn, error) {
	if dsn == "" {
		return nil, fmt.Errorf("dynamodb dsn was empty")
	}
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	awsConfig, err := aws.NewConfig(context.Background(), &cfg.Aws)
	if err != nil {
		return nil, err
	}

	return &Connection{
		cfg: awsConfig,
		client: dynamodb.NewFromConfig(*awsConfig, func(options *dynamodb.Options) {
			options.DefaultsMode = aws2.DefaultsModeLegacy
		}),
		executions: executions{maxSize: cfg.ExecMaxCache, cache: map[string]int{}},
	}, nil
}
