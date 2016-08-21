// +build !integration

package fastsql

import (
	"database/sql"
	"net/url"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("fastsql", func() {
	Describe("#Close", func() {
		var (
			err error
			dbh *DB
		)

		BeforeEach(func() {
			dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when a valid database connection is closed", func() {
			It("should not error", func() {
				err = dbh.Close()
				Expect(err).NotTo(HaveOccurred())
			})
		})
	}) // Describe #Close

	Describe(".newInsert", func() {
		var in *insert

		BeforeEach(func() {
			in = newInsert()
		})

		Context("", func() {
			It("", func() {
				Expect(len(in.bindParams)).To(BeZero())
				Expect(in.values).To(Equal(" VALUES"))
			})
		})
	}) // Describe .newInsert

	Describe("#Open", func() {
		const flushInterval uint = 100
		var (
			err error
			dbh *DB
		)

		BeforeEach(func() {
			dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err = dbh.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when opening a new fastsql database connection", func() {
			It("should properly set the flush interval", func() {
				Expect(dbh.flushInterval).To(Equal(flushInterval))
			})
		})
	}) // Describe #Open

	Describe("#SetDB", func() {
		var (
			err error
			dbh *DB
		)

		BeforeEach(func() {
			dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err = dbh.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when setting a database connection", func() {
			It("should not error", func() {
				dbhMock, _, err := sqlmock.New()

				defer dbhMock.Close()
				Expect(err).NotTo(HaveOccurred())

				err = dbh.setDB(dbhMock)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	}) // Describe #SetDB

	Describe("#splitQuery", func() {
		var (
			err     error
			query   string
			dbh     *DB
			dbhMock *sql.DB
		)

		BeforeEach(func() {
			dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100)
			Expect(err).NotTo(HaveOccurred())

			dbhMock, _, err = sqlmock.New()
			Expect(err).NotTo(HaveOccurred())
			dbh.setDB(dbhMock)
		})

		AfterEach(func() {
			// err = dbhMock.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		Context("with an insert query with a lowercase table name", func() {
			It("should build a valid query", func() {
				query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?);"

				if err = dbh.BatchInsert(
					query,
					[]interface{}{
						1,
						2,
						3,
					}...,
				); err != nil {
					Expect(err).NotTo(HaveOccurred())
				}

				Expect(dbh.batchInserts[query].queryPart1).To(Equal("INSERT INTO table_name(a, b, c)"))
				Expect(dbh.batchInserts[query].queryPart2).To(Equal("(?, ?, ?),"))
			})
		})

		Context("with an insert query with a mixed-case table name", func() {
			It("should build a valid query", func() {
				query = "INSERT INTO TaBle_NamE(a, b, c) VALUES(?, ?, ?);"

				if err = dbh.BatchInsert(
					query,
					[]interface{}{
						1,
						2,
						3,
					}...,
				); err != nil {
					Expect(err).NotTo(HaveOccurred())
				}

				Expect(dbh.batchInserts[query].queryPart1).To(Equal("INSERT INTO TaBle_NamE(a, b, c)"))
				Expect(dbh.batchInserts[query].queryPart2).To(Equal("(?, ?, ?),"))
			})
		})
	}) // Describe #splitQuery
}) // fastsql

func TestFlushInsert(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
		mock    sqlmock.Sqlmock
	)

	//t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, mock, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
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
		}
	}

	mock.ExpectPrepare("INSERT INTO table_name\\(a, b, c\\) VALUES\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\)")
	mock.ExpectExec("INSERT INTO table_name\\(a, b, c\\) VALUES\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\),\\(\\?, \\?, \\?\\)").
		WithArgs(1, 2, 3, 1, 2, 3, 1, 2, 3).
		WillReturnResult(sqlmock.NewResult(0, 3))

	if err = dbh.flushInsert(dbh.batchInserts[query]); err != nil {
		t.Fatal(err)
	}

	if dbh.batchInserts[query].values != " VALUES" {
		t.Fatal("dbh.values not properly reset by dbh.Flush().")
	}

	if len(dbh.batchInserts[query].bindParams) > 0 {
		t.Fatal("dbh.bindParams not properly reset by dbh.Flush().")
	}

	if dbh.batchInserts[query].insertCtr != 0 {
		t.Fatal("dbh.insertCtr not properly reset by dbh.Flush().")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestBatchInsert(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	)

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, _, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
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
		}
	}

	if len(dbh.batchInserts[query].bindParams) != 9 {
		t.Log(dbh.batchInserts[query].bindParams)
		t.Fatal("dbh.bindParams not properly set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].insertCtr != 3 {
		t.Log(dbh.batchInserts[query].insertCtr)
		t.Fatal("dbh.insertCtr not properly being set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(dbh.batchInserts[query].values)
		t.Fatal("dbh.values not properly being set by dbh.BatchInsert().")
	}
}

func TestBatchInsertOnDuplicateKeyUpdate(t *testing.T) {
	var (
		err     error
		query   string
		dbh     *DB
		dbhMock *sql.DB
	)

	t.Parallel()

	if dbh, err = Open("mysql", "user:pass@tcp(localhost:3306)/db_name?"+url.QueryEscape("charset=utf8mb4,utf8&loc=America/New_York"), 100); err != nil {
		t.Fatal(err)
	}
	defer dbh.Close()

	if dbhMock, _, err = sqlmock.New(); err != nil {
		t.Fatal(err)
	}
	defer dbhMock.Close()

	dbh.setDB(dbhMock)

	query = "INSERT INTO table_name(a, b, c) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE c = ?;"

	for i := 0; i < 3; i++ {
		if err = dbh.BatchInsert(
			query,
			[]interface{}{
				1,
				2,
				3,
				4,
			}...,
		); err != nil {
			t.Fatal(err)
		}
	}

	if len(dbh.batchInserts[query].bindParams) != 12 {
		t.Log(dbh.batchInserts[query].bindParams)
		t.Fatal("dbh.bindParams not properly set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].insertCtr != 3 {
		t.Log(dbh.batchInserts[query].insertCtr)
		t.Fatal("dbh.insertCtr not properly being set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].values != " VALUES(?, ?, ?),(?, ?, ?),(?, ?, ?)," {
		t.Log(dbh.batchInserts[query].values)
		t.Fatal("dbh.values not properly being set by dbh.BatchInsert().")
	}

	if dbh.batchInserts[query].queryPart3 != "ON DUPLICATE KEY UPDATE c = ?;" {
		t.Fatalf("queryPart3 set incorrectly as '%s'.", dbh.batchInserts[query].queryPart3)
	}
}
