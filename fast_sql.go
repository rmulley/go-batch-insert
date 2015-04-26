package fastsql

import (
	"database/sql"
	"errors"
	"runtime"
	"strings"
) //import

type DB struct {
	*sql.DB
	driverName string
	flushRate  uint
	insert     *Insert
	update     *Update
} //DB

type Insert struct {
	bindParams []interface{}
	ctr        uint
	queryPart1 string
	queryPart2 string
	values     string
} //Insert

type Update struct {
	bindParams []interface{}
	ctr        uint
	queryPart1 string
	queryPart2 string
	values     string
} //Update

type queryConverter interface {
	split() error
} //queryConverter

func (this *Insert) split(query string) error {
	var (
		err                  error
		ndxParens, ndxValues int
	) //var

	// Catch runtime errors
	// Use a ptr so that the error is returned from the split func
	defer func(err *error) {
		if rec := recover(); rec != nil {
			*err = errors.New(rec.(*runtime.TypeAssertionError).Error())
		} //if
	}(&err) //func

	// Normalize and split query
	query = strings.ToLower(query)
	ndxValues = strings.LastIndex(query, "values")
	ndxParens = strings.LastIndex(query, ")")

	// Save the first and second parts of the query separately for easier building later
	this.queryPart1 = strings.TrimSpace(query[:ndxValues])
	this.queryPart2 = query[ndxValues+6:ndxParens+1] + ","

	return err
} //splitQuery

// Open is the same as sql.Open, but returns an *fastsql.DB instead.
func Open(driverName, dataSourceName string, flushRate uint) (*DB, error) {
	var (
		err error
		dbh *sql.DB
	) //var

	if dbh, err = sql.Open(driverName, dataSourceName); err != nil {
		return nil, err
	} //if

	return &DB{
		DB:         dbh,
		driverName: driverName,
		flushRate:  flushRate,
		insert: &Insert{
			bindParams: make([]interface{}, 0),
			values:     " VALUES",
		},
		update: &Update{
			bindParams: make([]interface{}, 0),
			values:     " VALUES",
		},
	}, err
} //Open

// Close is the same as sql.Close, but Flush's all INSERTs and UPDATEs first.
func (this *DB) Close() error {
	var (
		err error
	) //var

	if err = this.flushInserts(); err != nil {
		return err
	} //if

	return this.DB.Close()
} //Close

func (this *DB) BatchInsert(query string, params ...interface{}) (err error) {
	// Only split out query the first time Insert is called
	if this.insert.queryPart1 == "" {
		this.insert.split(query)
	} //if

	this.insert.ctr++

	// Build VALUES seciton of query and add to parameter slice
	this.insert.values += this.insert.queryPart2
	this.insert.bindParams = append(this.insert.bindParams, params...)

	// If the batch interval has been hit, execute a batch insert
	if this.insert.ctr >= this.flushRate {
		err = this.flushInserts()
	} //if

	return err
} //BatchInsert

func (this *DB) flushInserts() error {
	var (
		err  error
		stmt *sql.Stmt
	) //var

	if stmt, err = this.DB.Prepare(this.insert.queryPart1 + this.insert.values[:len(this.insert.values)-1]); err != nil {
		return (err)
	} //if
	defer stmt.Close()

	if _, err = stmt.Exec(this.insert.bindParams...); err != nil {
		return (err)
	} //if

	// Reset vars
	_ = stmt.Close()
	this.insert.values = " VALUES"
	this.insert.bindParams = make([]interface{}, 0)
	this.insert.ctr = 0

	return err
} //flushInserts

func (this *DB) flushUpdates() error {
	var (
		err  error
		stmt *sql.Stmt
	) //var

	if stmt, err = this.DB.Prepare(this.insert.queryPart1 + this.insert.values[:len(this.insert.values)-1]); err != nil {
		return (err)
	} //if
	defer stmt.Close()

	if _, err = stmt.Exec(this.insert.bindParams...); err != nil {
		return (err)
	} //if

	// Reset vars
	_ = stmt.Close()
	this.insert.values = " VALUES"
	this.insert.bindParams = make([]interface{}, 0)
	this.insert.ctr = 0

	return err
} //flushInserts

func (this *DB) SetDB(dbh *sql.DB) (err error) {
	if err = dbh.Ping(); err != nil {
		return err
	} //if

	this.DB = dbh
	return nil
} //SetDB

/*
func (this *DB) Update(query string, params ...interface{}) (err error) {
	// Only split out query the first time Update is called
	if this.updateQueryPart1 == "" {
		this.splitQuery(query)
	} //if

	this.updateCtr++

	// Build VALUES seciton of query and add to parameter slice
	this.values += this.updateQueryPart2
	this.bindParams = append(this.bindParams, params...)

	// If the batch interval has been hit, execute a batch update
	if this.updateCtr >= this.flushRate {
		err = Flusher(this).Flush()
	} //if

	return err
} //Update
*/
