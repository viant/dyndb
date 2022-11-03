# Datastore Connectivity for DynamoDB(dyndb)


[![DynamoDB database/sql driver](https://goreportcard.com/badge/github.com/viant/dyndb)](https://goreportcard.com/report/github.com/viant/dyndb)
[![GoDoc](https://godoc.org/github.com/viant/dyndb?status.svg)](https://godoc.org/github.com/viant/dyndb)

This library is compatible with Go 1.17+


Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Usage](#Usage)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)





## Usage:


The following is a very simple example of CRUD operations

```go
package main

import (
	"database/sql"
	"log"
	_ "github.com/viant/dyndb"
)

type Participant struct {
	Name   string
	Splits []float64
}

func main() {

	db, err := sql.Open("dynamodb", "dynamodb://region/?credURL=")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	

```

<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:**

**Contributors:**