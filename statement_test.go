package dyndb

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/toolbox"
	"testing"
)

func TestStatement_Query(t *testing.T) {

	//dsn := os.Getenv("TEST_DSN")
	//db, err := sql.Open("dynamodb", dsn)
	db, err := sql.Open("dynamodb", "dynamodb://localhost:8000/us-west-1?cred=aws-e2e")
	if err != nil {
		t.Skipf("failed to connect to db %v", err)
		return
	}
	//db, err := sql.Open("dynamodb", "dynamodb://aws/us-west-1?cred=aws-e2e")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	type Publication struct {
		ISBN       string
		Name       string
		Published  int
		Status     int
		Categories []string
		IsTravel   bool
		IsFinance  bool
	}

	var testCases = []struct {
		description string
		initSQL     []string
		SQL         string
		append      func(rows *sql.Rows, records *[]interface{}) error
		expect      string
	}{
		{
			description: "simple select",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, CATEGORIES) VALUES('AAA-BBB', 'Title 1', 20020121, 1, ARRAY('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status) VALUES('AAA-CCB', 'Title 2', 20020122, 1)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status) VALUES('AAA-XCB', 'Title 3', 20020124, 0)`,
			},
			SQL: "SELECT ISBN, Name, Published, INT(Status) AS Status FROM Publication ",
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name, &record.Published, &record.Status)
				*records = append(*records, record)
				return err
			},
			expect: `[
	{"@indexBy@":"ISBN"},
	{
		"ISBN": "AAA-XCB",
		"Name": "Title 3",
		"Published": 20020124,
		"Status": 0
	},
	{
		"ISBN": "AAA-CCB",
		"Name": "Title 2",
		"Published": 20020122,
		"Status": 1
	},
	{
		"ISBN": "AAA-BBB",
		"Name": "Title 1",
		"Published": 20020121,
		"Status": 1
	}
]`,
		},
		{
			description: "custom function",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-BBB', 'Title 1', 20020121, 1, LIST('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-XXX', 'Title 2', 20020121, 1, LIST('FINANCE'))`,
			},
			SQL: `SELECT ISBN, Name, 
       				ARRAY_EXISTS(Categories, 'TRAVEL') AS IS_TRAVEL ,
					ARRAY_EXISTS(Categories, 'FINANCE') AS IS_FINANCE 
					FROM Publication`,
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name, &record.IsTravel, &record.IsFinance)
				*records = append(*records, record)
				return err
			},
			expect: `[{"@indexBy@":"ISBN"},
	{
		"ISBN": "AAA-XXX",
		"Name": "Title 2",
		"IsTravel": false,
		"IsFinance": true
	},
	{
		"ISBN": "AAA-BBB",
		"IsTravel": true,
		"IsFinance": true
	}
]`,
		},

		{
			description: "query wrapper",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-BBB', 'Title 1', 20020121, 1, ARRAY('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-XXX', 'Title 2', 20020121, 1, ARRAY('FINANCE'))`,
			},
			SQL: `SELECT * FROM (SELECT ISBN, Name FROM Publication) t WHERE 1=1`,
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name)
				*records = append(*records, record)
				return err
			},
			expect: `[{"@indexBy@":"ISBN"},
	{
		"ISBN": "AAA-XXX",
		"Name": "Title 2"
	},
	{
		"ISBN": "AAA-BBB",
		"Name": "Title 1"
	}
]`,
		},
	}

outer:
	for _, testCase := range testCases {

		for _, SQL := range testCase.initSQL {
			_, err := db.Exec(SQL)
			if !assert.Nil(t, err, testCase.description) {
				fmt.Println(err)
				fmt.Println(SQL)
				break outer
			}
		}

		stmt, err := db.Prepare(testCase.SQL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		rows, err := stmt.Query()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		var actual []interface{}
		for rows.Next() {
			err = testCase.append(rows, &actual)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}
		}
		assert.Nil(t, rows.Err(), testCase.description)
		if !assertly.AssertValues(t, testCase.expect, actual, testCase.description) {
			toolbox.DumpIndent(actual, true)
		}
	}

}
