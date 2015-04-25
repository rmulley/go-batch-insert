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

	if dbh.values != " VALUES" {
		t.Fatal("'values' not being set correctly in Open().")
	} //if
} //TestOpen

func TestFlush(t *testing.T) {
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

	dbh.SetDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	for i := 0; i < 3; i++ {
		if err = dbh.Insert(
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

	if err = dbh.Flush(); err != nil {
		t.Fatal(err)
	} //if

	if dbh.values != " VALUES" {
		t.Fatal("dbh.values not properly reset by dbh.Flush().")
	} //if

	if len(dbh.bindParams) > 0 {
		t.Fatal("dbh.bindParams not properly reset by dbh.Flush().")
	} //if

	if dbh.insertCtr != 0 {
		t.Fatal("dbh.insertCtr not properly reset by dbh.Flush().")
	} //if
} //TestFlush

func TestInsert(t *testing.T) {
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

	dbh.SetDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	for i := 0; i < 3; i++ {
		if err = dbh.Insert(
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

	if len(dbh.bindParams) != 9 {
		t.Log(dbh.bindParams)
		t.Fatal("dbh.bindParams not properly set by dbh.Insert().")
	} //if

	if dbh.insertCtr != 3 {
		t.Log(dbh.insertCtr)
		t.Fatal("dbh.insertCtr not properly being set by dbh.Insert().")
	} //if

	if dbh.values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(dbh.values)
		t.Fatal("dbh.values not properly being set by dbh.Insert().")
	} //if
} //TestInsert

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

	if err = dbh.SetDB(dbhMock); err != nil {
		t.Fatal(err)
	} //if
} //TestSetDB

func TestSplitQuery(t *testing.T) {
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

	dbh.SetDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

	if err = dbh.Insert(
		query,
		[]interface{}{
			1,
			2,
			3,
		}...,
	); err != nil {
		t.Fatal(err)
	} //if

	if dbh.queryPart1 != "insert into table_name(a, b, c)" {
		t.Log("*" + dbh.queryPart1 + "*")
		t.Fatal("dbh.queryPart1 not formatted correctly.")
	} //if

	if dbh.queryPart2 != "(?, ?, ?)," {
		t.Log("*" + dbh.queryPart2 + "*")
		t.Fatal("dbh.queryPart2 not formatted correctly.")
	} //if
} //TestSplitQuery
