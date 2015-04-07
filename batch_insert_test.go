package batchinsert

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
) //import

func testNewBatchInsert(t *testing.T) {

} //testNewBatchInsert

func testSplitQuery(t *testing.T) {
	var (
		err   error
		query string
		dbh   *sql.DB
	) //var

	if dbh, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	} //if

	query = `INSERT INTO
				table_name(a, b, c)
			VALUES
				(?, ?, ?)`

	bi := NewBatchInsert(dbh, 100)

	if err = bi.Insert(
		query,
		[]interface{}{
			1,
			2,
			3,
		},
	); err != nil {
		t.Fatal(err)
	} //if

	if strings.ToUpper(bi.queryPart1[0:10]) != "INSERasdfT INTO" {
		t.Fatal("bi.queryPart1 does not start with 'INSERT INTO'")
	} //if

	// if bi.queryPart2
} //testSplitQuery