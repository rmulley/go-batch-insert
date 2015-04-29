package fastsql

import (
	"database/sql"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
) //import

func TestOpen(t *testing.T) {
	var (
		err       error
		flushRate uint = 100
		dbh       *DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if

	if dbh.flushRate != flushRate {
		t.Fatal("'insertRate' not being set correctly in Open().")
	} //if

	if dbh.insert.values != " VALUES" {
		t.Fatal("'values' not being set correctly in Open().")
	} //if
} //TestOpen

func TestFlushUpdates(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if
	defer dbh.Close()

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "UPDATE table_name SET field1 = ?, field2 = ? WHERE field3 = ?;"

	for i := 0; i < 3; i++ {
		if err = dbh.BatchUpdate(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		} //if
	} //for

	/*
	   UPDATE mytable
	   SET
	     mytext = myvalues.mytext,
	     myint = myvalues.myint
	   FROM (
	     VALUES
	       (1, 'textA', 99),
	       (2, 'textB', 88),
	       ...
	   ) AS myvalues (mykey, mytext, myint)
	   WHERE mytable.mykey = myvalues.mykey
	*/

	sqlmock.ExpectExec("update table_name set field1 = myVals.field1, myVals.field2 FROM \\(VALUES\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\)\\) as myvals(key, field1, field2) where field3 = myvalues.key;").
		WithArgs(1, 2, 3, 1, 2, 3, 1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err = dbh.flushInserts(); err != nil {
		t.Fatal(err)
	} //if

	/*
		if dbh.insert.values != " VALUES" {
			t.Fatal("dbh.values not properly reset by dbh.Flush().")
		} //if

		if len(dbh.insert.bindParams) > 0 {
			t.Fatal("dbh.bindParams not properly reset by dbh.Flush().")
		} //if

		if dbh.insert.ctr != 0 {
			t.Fatal("dbh.insertCtr not properly reset by dbh.Flush().")
		} //if
	*/
} //TestFlushUPdates

func TestFlushInserts(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if
	defer dbh.Close()

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	for i := 0; i < 3; i++ {
		if err = dbh.BatchInsert(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		} //if
	} //for

	sqlmock.ExpectExec("insert into table_name\\(a, b, c\\) VALUES\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\)").
		WithArgs(1, 2, 3, 1, 2, 3, 1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err = dbh.flushInserts(); err != nil {
		t.Fatal(err)
	} //if

	if dbh.insert.values != " VALUES" {
		t.Fatal("dbh.values not properly reset by dbh.Flush().")
	} //if

	if len(dbh.insert.bindParams) > 0 {
		t.Fatal("dbh.bindParams not properly reset by dbh.Flush().")
	} //if

	if dbh.insert.ctr != 0 {
		t.Fatal("dbh.insertCtr not properly reset by dbh.Flush().")
	} //if
} //TestFlushInserts

func TestBatchInsert(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	for i := 0; i < 3; i++ {
		if err = dbh.BatchInsert(
			query,
			[]interface{}{
				1,
				2,
				3,
			}...,
		); err != nil {
			t.Fatal(err)
		} //if
	} //for

	if len(dbh.insert.bindParams) != 9 {
		t.Log(dbh.insert.bindParams)
		t.Fatal("dbh.insert.bindParams not properly set by dbh.BatchInsert().")
	} //if

	if dbh.insert.ctr != 3 {
		t.Log(dbh.insert.ctr)
		t.Fatal("dbh.insert.ctr not properly being set by dbh.BatchInsert().")
	} //if

	if dbh.insert.values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(dbh.insert.values)
		t.Fatal("dbh.insert.values not properly being set by dbh.BatchInsert().")
	} //if
} //TestBatchInsert

func (this *DB) TestSetDB(t *testing.T) {
	var (
		err     error
		dbhMock *sql.DB
		dbh     *DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if
	defer dbhMock.Close()

	if err = dbh.setDB(dbhMock); err != nil {
		t.Fatal(err)
	} //if
} //TestSetDB

func TestParseQuery(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	if err = dbh.BatchInsert(
		query,
		[]interface{}{
			1,
			2,
			3,
		}...,
	); err != nil {
		t.Fatal(err)
	} //if

	if dbh.insert.queryPart1 != "insert into table_name(a, b, c)" {
		t.Log("*" + dbh.insert.queryPart1 + "*")
		t.Fatal("dbh.insert.queryPart1 not formatted correctly.")
	} //if

	if dbh.insert.queryPart2 != "(?, ?, ?)," {
		t.Log("*" + dbh.insert.queryPart2 + "*")
		t.Fatal("dbh.insert.queryPart2 not formatted correctly.")
	} //if
} //TestParseQuery

func TestParseQuery2(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	) //var

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	} //if

	if dbhMock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "UPDATE table_name SET field1 = ?, field2 = ? WHERE field3 = ?;"

	if err = dbh.BatchUpdate(
		query,
		[]interface{}{
			1,
			2,
			3,
		}...,
	); err != nil {
		t.Fatal(err)
	} //if

	if dbh.update.queryPart1 != "update table_name" {
		t.Log("*" + dbh.update.queryPart1 + "*")
		t.Fatal("dbh.update.queryPart1 not formatted correctly.")
	} //if

	if dbh.update.queryPart2 != "set field1 = myVals.field1, field2 = myVals.field2" {
		t.Log("*" + dbh.update.queryPart2 + "*")
		t.Fatal("dbh.update.queryPart2 not formatted correctly.")
	} //if

	if dbh.update.queryPart3 != "(?, ?, ?)," {
		t.Log("*" + dbh.update.queryPart3 + "*")
		t.Fatal("dbh.update.queryPart3 not formatted correctly.")
	} //if
} //TestParseQuery2
