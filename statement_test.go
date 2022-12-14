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

	dsn := "dynamodb://localhost:8000/us-west-1?key=dummy&secret=dummy"
	//dsn := os.Getenv("TEST_DSN")
	db, err := sql.Open("dynamodb", dsn)
	if dsn == "" {
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
		M          map[string]interface{}
	}

	var testCases = []struct {
		description string
		initSQL     []string
		args        []interface{}
		SQL         string
		append      func(rows *sql.Rows, records *[]interface{}) error
		expect      string
		skip        bool
	}{

		{
			description: "coalesce function",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-BBB', 'Title 1', 20020121, 1, LIST('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-XXX', 'Title 2', 20020121, 1, LIST('FINANCE'))`,
			},
			SQL: `SELECT ISBN, Name, COALESCE(IsTravel, false) AS IsTravel, COALESCE(IsFinance, false) AS IsFinance 
				FROM (  
					SELECT 
					    ISBN,
					    Name, 
       					ARRAY_EXISTS(Categories, 'TRAVEL') AS IsTravel ,
						ARRAY_EXISTS(Categories, 'FINANCE') AS IsFinance 
					FROM Publication WHERE ISBN = ?)`,
			args: []interface{}{"AAA-XXX"},
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
	}
]`,
		},
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
			description: "simple select with limit",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, CATEGORIES) VALUES('AAA-BBB', 'Title 1', 20020121, 1, ARRAY('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status) VALUES('AAA-CCB', 'Title 2', 20020122, 1)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status) VALUES('AAA-XCB', 'Title 3', 20020124, 0)`,
			},
			SQL: "SELECT ISBN, Name, Published, INT(Status) AS Status FROM Publication WHERE Status = 1 LIMIT 2",
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name, &record.Published, &record.Status)
				*records = append(*records, record)
				return err
			},
			expect: `[
	{"@indexBy@":"ISBN"},
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

		{
			description: "query single with placeholder",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-BBB', 'Title 1', 20020121, 1, ARRAY('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-XXX', 'Title 2', 20020121, 1, ARRAY('FINANCE'))`,
			},
			SQL:  `SELECT ISBN, Name FROM Publication t WHERE (ISBN = ?) AND  1=1`,
			args: []interface{}{"AAA-BBB"},
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name)
				*records = append(*records, record)
				return err
			},
			expect: `[
	{
		"ISBN": "AAA-BBB",
		"Name": "Title 1"
	}
]`,
		},
		{
			description: "query single ",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-BBB', 'Title 1', 20020121, 1, ARRAY('TRAVEL', 'FINANCE'))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, Categories) VALUES('AAA-XXX', 'Title 2', 20020121, 1, ARRAY('FINANCE'))`,
			},
			SQL:  `SELECT ISBN, Name FROM Publication t WHERE ISBN IN( ?, ?) AND 1=1`,
			args: []interface{}{"AAA-BBB", "AAA-XXW"},
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name)
				*records = append(*records, record)
				return err
			},
			expect: `[
	{
		"ISBN": "AAA-BBB",
		"Name": "Title 1"
	}
]`,
		},

		{
			description: "query with map",
			initSQL: []string{
				`DROP TABLE IF EXISTS Publication`,
				`CREATE TABLE IF NOT EXISTS Publication(
				ISBN TEXT HASH KEY,
				Published INT RANGE KEY)`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, InfoMap) VALUES('AAA-BBB', 'Title 1', 20020121, 1, Map({'k1':[1,2,3]}))`,
				`INSERT INTO Publication(ISBN, Name, Published, Status, InfoMap) VALUES('AAA-XXX', 'Title 2', 20020121, 1, Map({'k2':[1,2,3]}))`,
			},
			SQL: `SELECT ISBN, Name, InfoMap FROM Publication`,
			append: func(rows *sql.Rows, records *[]interface{}) error {
				record := &Publication{}
				err = rows.Scan(&record.ISBN, &record.Name, &record.M)
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
			skip: true,
		},
	}

outer:
	for _, testCase := range testCases {
		if testCase.skip {
			t.Skip(testCase.description)
			continue
		}
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
		rows, err := stmt.Query(testCase.args...)
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
