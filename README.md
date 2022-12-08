# Dynamodb SQL Driver


[![DynamoDB database/sql driver](https://goreportcard.com/badge/github.com/viant/dyndb)](https://goreportcard.com/report/github.com/viant/dyndb)
[![GoDoc](https://godoc.org/github.com/viant/dyndb?status.svg)](https://godoc.org/github.com/viant/dyndb)

This library is compatible with Go 1.17+


Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.


Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [DSN](#dsn-data-source-name)
- [Usage](#usage)
- [Benchmark](#benchmark)
- [Bugs](#bugs)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)


This library provides fast implementation of the DynamoDB as a database/sql driver.
For most of the operation this driver uses PartiSQL  with ability to define custom functions.


#### DSN Data Source Name

The Dynamodb driver accepts the following DSN

* 'dynamodb://aws|{dockerEndpoint}/{region}/[{options}]'

  Where queryString can optionally configure the following option:
    - key:  access key id
    - secret: access key secret
    - credURL: (url encoded) local location or URL supported by  [Scy](https://github.com/viant/scy)
    - credKey: optional (url encoded) [Scy](https://github.com/viant/scy) secret manager key or key location
    - credID: [Scy](https://github.com/viant/scy) resource secret ID
    - roleArn, session to use assumed role


## Usage:


The following is a very simple example of CRUD operations

```go
package main

import (
  "context"
  "database/sql"
  "encoding/json"
  "fmt"
  "log"
  _ "github.com/viant/dyndb"
  "time"
)

type Publication struct {
  ISBN      string
  Name      string
  IsTravel  bool
  IsFinance bool
}

func main() {

  db, err := sql.Open("dynamodb", "dynamodb://localhost:8000/")
  if err != nil {
    log.Fatalln(err)
  }
  defer db.Close()
  SQL := `SELECT ISBN, Name,
		ARRAY_EXISTS(Categories, 'TRAVEL') AS IS_TRAVEL ,
		ARRAY_EXISTS(Categories, 'FINANCE') AS IS_FINANCE
	FROM Publication`

  ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
  defer cancel()
  stmt, err := db.PrepareContext(ctx, SQL)
  if err != nil {
    log.Fatalln(err)
  }
  rows, err := stmt.Query()
  if err != nil {
    log.Fatalln(err)
  }
  var records []*Publication
  for rows.Next() {
    record := &Publication{}
    err = rows.Scan(&record.ISBN, &record.Name, &record.IsFinance, &record.IsTravel)
    if err != nil {
      log.Fatalln(err)
    }
    records = append(records, record)
  }
  data, _ := json.Marshal(records)
  fmt.Printf("%s\n", data)

}

```


## Benchmark

Benchmark runs times the following query:

- QueryAll: (fetches 1000 records)
```sql 
   SELECT id, state,gender,year,name, number  FROM usa_names
```

```text
BenchmarkDatabaseSQL_QueryAll
BenchmarkDatabaseSQL_QueryAll-16       	      80	  14983399 ns/op	 1080045 B/op	   18194 allocs/op
BenchmarkAwsSDK_QueryAll
BenchmarkAwsSDK_QueryAll-16            	      54	  19998584 ns/op	 3654102 B/op	   51359 allocs/op
```


- QuerySingle: (fetches 1 record)
```sql 
   SELECT id, state,gender,year,name, number  FROM usa_names WHERE id = 1
```
```text
  BenchmarkDatabaseSQL_QuerySingle
  BenchmarkDatabaseSQL_QuerySingle-16    	     726	   1651893 ns/op	   24465 B/op	     333 allocs/op
  BenchmarkAwsSDK_QuerySingle
  BenchmarkAwsSDK_QuerySingle-16         	     795	   1682389 ns/op	   29651 B/op	     374 allocs/op
```




In both case database/sql driver is faster and allocate way less memory 
than native [AWS SDK client](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/dynamodb)


## Bugs

This package implement only basic SQL with limited functionality. 
It extends original PartiSQL with extra client side functionality.
Contributors are welcome.


<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:**

**Contributors:**