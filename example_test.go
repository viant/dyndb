package dyndb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func ExampleConnection_PrepareContext() {
	type Publication struct {
		ISBN      string
		Name      string
		IsTravel  bool
		IsFinance bool
	}
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
