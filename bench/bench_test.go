package bench_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	_ "github.com/viant/dyndb"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type Demo struct {
	ID     int
	State  string
	Gender string
	Year   string
	Name   string
	Number string
	N      int
}

func init() {
	var err error
	startTime := time.Now()

	dsn := os.Getenv("TEST_DSN")
	db, err := sql.Open("dynamodb", dsn)
	//db, err := sql.Open("dynamodb", "dynamodb://localhost:8000/us-west-1?cred=aws-e2e")
	if err != nil {
		return
	}

	if err == nil {
		err = loadTestData(context.Background(), db, "usa_names", "data.json.gz")
	}
	fmt.Printf("loaded data after: %v %v\n", time.Since(startTime), err)

}

func loadTestData(ctx context.Context, db *sql.DB, dest string, source string) error {
	fmt.Printf("loading data into: %v\n...\n", dest)
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, source)
	if err != nil {
		return err
	}
	if strings.HasSuffix(source, ".gz") {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return err
		}
		data, _ = ioutil.ReadAll(reader)
	}

	var records []*Demo
	if err = json.Unmarshal(data, &records); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS  usa_names"); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	DDL := `CREATE TABLE usa_names(id INT HASH KEY)`
	if _, err = db.ExecContext(ctx, DDL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	SQL := `INSERT INTO usa_names(id, state, gender, year, name, number) VALUES(?, ?, ?, ?, ?, ?)`
	stmt, err := db.PrepareContext(ctx, SQL)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, item := range records {
		item.ID = i
		num, _ := strconv.Atoi(item.Number)
		if _, err = stmt.Exec(item.ID, item.State, item.Gender, item.Year, item.Name, num); err != nil {
			return err
		}
		if i == 999 {
			break
		}
	}
	return nil
}

func BenchmarkDatabaseSQL_QueryAll(b *testing.B) {

	dsn := os.Getenv("TEST_DSN")
	db, err := sql.Open("dynamodb", dsn)
	//db, err := sql.Open("dynamodb", "dynamodb://localhost:8000/us-west-1?cred=aws-e2e")
	if err != nil {
		b.Skipf("failed to connect to db %w", err)
	}
	if !assert.Nil(b, err) {
		return
	}
	b.ReportAllocs()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {

		stmt, err := db.PrepareContext(ctx, `SELECT id, state,gender,year,name, INT(number) AS number  FROM usa_names`)
		if !assert.Nil(b, err) {
			continue
		}
		rows, err := stmt.Query()
		if !assert.Nil(b, err) {
			continue
		}
		count := 0
		for rows.Next() {
			demo := &Demo{}
			if err = rows.Scan(&demo.ID, &demo.State, &demo.Gender, &demo.Year, &demo.Number, &demo.N); err != nil {
				assert.Nil(b, err)
			}
			count++
		}
		assert.Equal(b, 1000, count)
	}
}

func BenchmarkDatabaseSQL_QuerySingle(b *testing.B) {
	dsn := os.Getenv("TEST_DSN")
	db, err := sql.Open("dynamodb", dsn)
	//db, err := sql.Open("dynamodb", "dynamodb://localhost:8000/us-west-1?cred=aws-e2e")
	if err != nil {
		b.Skipf("failed to connect to db %w", err)
	}
	if !assert.Nil(b, err) {
		return
	}
	b.ReportAllocs()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {

		stmt, err := db.PrepareContext(ctx, `SELECT id, state,gender,year,name, INT(number) AS number  FROM usa_names WHERE id =1`)
		if !assert.Nil(b, err) {
			continue
		}
		rows, err := stmt.Query()
		if !assert.Nil(b, err) {
			continue
		}
		count := 0
		for rows.Next() {
			demo := &Demo{}
			if err = rows.Scan(&demo.ID, &demo.State, &demo.Gender, &demo.Year, &demo.Number, &demo.N); err != nil {
				assert.Nil(b, err)
			}
			count++
		}
		assert.Equal(b, 1, count)
	}
}

func BenchmarkAwsSDK_QueryAll(b *testing.B) {

	client := createLocalClient()
	ctx := context.Background()

	SQL := `SELECT id, state,gender,year,name, number  FROM usa_names `
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		limit := int32(100)
		output, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
			Statement: &SQL,
			Limit:     &limit,
		})
		if !assert.Nil(b, err) {
			fmt.Println(err)
			continue
		}

		for _, item := range output.Items {
			var demos Demo
			if err := attributevalue.UnmarshalMap(item, &demos); err != nil {
				assert.Nil(b, err)
			}
		}
		assert.Equal(b, 1000, len(output.Items))

	}

}

func BenchmarkAwsSDK_QuerySingle(b *testing.B) {

	client := createLocalClient()
	ctx := context.Background()

	SQL := `SELECT id, state,gender,year,name, number  FROM usa_names WHERE id = 1`
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		limit := int32(100)
		output, err := client.ExecuteStatement(ctx, &dynamodb.ExecuteStatementInput{
			Statement: &SQL,
			Limit:     &limit,
		})
		if !assert.Nil(b, err) {
			fmt.Println(err)
			continue
		}

		for _, item := range output.Items {
			var demos Demo
			if err := attributevalue.UnmarshalMap(item, &demos); err != nil {
				assert.Nil(b, err)
			}
		}
		assert.Equal(b, 1, len(output.Items))

	}

}

func createLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-west-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000"}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "dummy", SecretAccessKey: "dummy", SessionToken: "dummy",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		panic(err)
	}

	return dynamodb.NewFromConfig(cfg)
}
