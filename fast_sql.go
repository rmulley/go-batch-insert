package fastsql

import (
	"database/sql"
	"strings"
) //import

type DB struct {
	*sql.DB
	driverName string
	bindParams []interface{}
	insertCtr  uint
	flushRate  uint
	queryPart1 string
	queryPart2 string
	values     string
} //DB

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
		bindParams: make([]interface{}, 0),
		flushRate:  flushRate,
		values:     " VALUES",
	}, err
} //Open

func (this *DB) Insert(query string, params ...interface{}) (err error) {
	// Only split out query the first time Insert is called
	if this.queryPart1 == "" {
		this.splitQuery(query)
	} //if

	this.insertCtr++

	// Build VALUES seciton of query and add to parameter slice
	this.values += this.queryPart2
	this.bindParams = append(this.bindParams, params...)

	// If the batch interval has been hit, execute a batch insert
	if this.insertCtr >= this.flushRate {
		err = this.Flush()
	} //if

	return err
} //Insert

func (this *DB) Flush() (err error) {
	var (
		stmt *sql.Stmt
	) //var

	// Prepare query
	if stmt, err = this.DB.Prepare(this.queryPart1 + this.values[:len(this.values)-1]); err != nil {
		return (err)
	} //if
	defer stmt.Close()

	// Executate batch insert
	if _, err = stmt.Exec(this.bindParams...); err != nil {
		return (err)
	} //if

	// Reset vars
	_ = stmt.Close()
	this.values = " VALUES"
	this.bindParams = make([]interface{}, 0)
	this.insertCtr = 0

	return err
} //Flush

func (this *DB) SetDB(dbh *sql.DB) (err error) {
	if err = dbh.Ping(); err != nil {
		return err
	} //if

	this.DB = dbh
	return nil
} //SetDB

func (this *DB) Update(query string, params ...interface{}) (err error) {
	// Only split out query the first time Update is called
	if this.queryPart1 == "" {
		this.splitQuery(query)
	} //if

	this.insertCtr++

	// Build VALUES seciton of query and add to parameter slice
	this.values += this.queryPart2
	this.bindParams = append(this.bindParams, params...)

	// If the batch interval has been hit, execute a batch update
	if this.insertCtr >= this.flushRate {
		err = this.Flush()
	} //if

	return err
} //Update

func (this *DB) splitQuery(query string) {
	var (
		ndxParens, ndxValues int
	) //var

	// Normalize and split query
	query = strings.ToLower(query)
	ndxValues = strings.LastIndex(query, "values")
	ndxParens = strings.LastIndex(query, ")")

	// Save the first and second parts of the query separately for easier building later
	this.queryPart1 = strings.TrimSpace(query[:ndxValues])
	this.queryPart2 = query[ndxValues+6:ndxParens+1] + ","
} //splitQuery
