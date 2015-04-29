package fastsql

import (
	"database/sql"
	"errors"
	//"log"
	"regexp"
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
	queryPart1 string // INSERT INTO table_name(field1, field2)
	queryPart2 string // (?, ?),
	values     string // VALUES(?, ?), (?, ?)
} //Insert

type Update struct {
	bindParams []interface{}
	ctr        uint
	queryPart1 string // UPDATE table_name
	queryPart2 string // SET field1 = myVals.field1, field2 = myVals.field2
	queryPart3 string // FROM( VALUES(1, val1, val2), (2, val3, val4))
	queryPart4 string // AS myVals(key, field1, field2)
	queryPart5 string // WHERE table_name.key = myVals.key
	values     string
} //Update

func (this *Insert) parseQuery(query string) error {
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
} //parseQuery

func (this *Update) parseQuery2(query string) error {
	var (
		err              error
		ndxSet, ndxWhere int
		re               *regexp.Regexp
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
	ndxSet = strings.LastIndex(query, " set ")
	ndxWhere = strings.LastIndex(query, "where")

	// Save the first and second parts of the query separately for easier building later
	this.queryPart1 = strings.TrimSpace(query[:ndxSet])
	this.queryPart2 = strings.TrimSpace(query[ndxSet:ndxWhere])
	this.queryPart5 = strings.TrimSpace(query[ndxWhere:])

	// Reformat SET section of query
	re = regexp.MustCompile("(\\w+)\\s*=\\s*\\?")
	this.queryPart2 = re.ReplaceAllString(this.queryPart2, "$1 = myVals.$1")

	return err
} //parseQuery

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
func (this *DB) Close() (err error) {
	if err = this.flushInserts(); err != nil {
		return err
	} //if

	if err = this.flushUpdates(); err != nil {
		return err
	} //if

	return this.DB.Close()
} //Close

func (this *DB) BatchInsert(query string, params ...interface{}) (err error) {
	// Only split out query the first time function is called
	if this.insert.queryPart1 == "" {
		this.insert.parseQuery(query)
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

func (this *DB) BatchUpdate(query string, params ...interface{}) (err error) {
	// Only split out query the first time function is called
	if this.update.queryPart1 == "" {
		this.update.parseQuery2(query)
	} //if

	this.update.ctr++

	// Build VALUES seciton of query and add to parameter slice
	this.update.values += this.update.queryPart3
	this.update.bindParams = append(this.update.bindParams, params...)

	// If the batch interval has been hit, execute a batch update
	if this.update.ctr >= this.flushRate {
		err = this.flushUpdates()
	} //if

	return err
} //BatchUpdate

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

	if stmt, err = this.DB.Prepare(this.update.queryPart1 + this.update.queryPart2 + this.update.values[:len(this.update.values)-1]); err != nil {
		return (err)
	} //if
	defer stmt.Close()

	if _, err = stmt.Exec(this.update.bindParams...); err != nil {
		return (err)
	} //if

	// Reset vars
	_ = stmt.Close()
	this.update.values = " VALUES"
	this.update.bindParams = make([]interface{}, 0)
	this.update.ctr = 0

	return err
} //flushUpdates

func (this *DB) setDB(dbh *sql.DB) (err error) {
	if err = dbh.Ping(); err != nil {
		return err
	} //if

	this.DB = dbh
	return nil
} //setDB
