![Build Status](https://circleci.com/gh/rmulley/go-fast-sql.svg?style=shield)
[![Test 
[![GoDoc](https://godoc.org/github.com/rmulley/go-fast-sql?status.svg)](https://godoc.org/github.com/rmulley/go-fast-sql)
[![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/rmulley/go-fast-sql/master/LICENSE)
# go-fast-sql
Package fastsql is a library which extends Go's standard [database/sql](https://golang.org/pkg/database/sql/) library.  It provides performance that's easy to take advantage of.

Even better, the fastsql.DB object embeds the standard sql.DB object meaning access to all the standard database/sql library functionality is preserved.  It also means that integrating fastsql into existing codebases is a breeze.

Additional functionality inclues:
  1. Easy, readable, and performant batch insert queries using the BatchInsert method.
  2. Automatic creation and re-use of prepared statements.
  3. A convenient holder for manually used prepared statements.

##Example usage

```go
package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/rmulley/go-fast-sql"
	"log"
	"net/url"
)

func main() {
	var (
		err error
		i   uint = 1
		dbh *fastsql.DB
	)

	// Create new FastSQL DB object with a flush-interval of 100 rows
	if dbh, err = fastsql.Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		log.Fatalln(err)
	}
	defer dbh.Close()

	// Some loop performing SQL INSERTs
	for i <= 250 {
		if err = dbh.BatchInsert("INSERT INTO test_table(id, id2, id3) VALUES(?, ?, ?);", i, i + 1, i + 2); err != nil {
			log.Fatalln(err)
		}

		i++
	}
}
```
