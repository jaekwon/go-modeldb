package modeldb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
)

//-----------------------------------------------------------------------------
// INIT

// A global db instance, for convenience
var _db *sql.DB

// Set one here once per app
func SetDB(db *sql.DB) {
	_db = db
}

// To get the bare db instance should you need it
func GetDB() *sql.DB {
	return _db
}

func GetModelDB() *ModelDB {
	return &ModelDB{GetDB()}
}

//-----------------------------------------------------------------------------
// HELPER INTERFACES

// Common interface between *sql.Row and *sql.Rows
type RowScanner interface {
	Scan(dest ...interface{}) error
}

// Common interface between *sql.Tx and *sql.DB
type Conn interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Common interface between *ModelTx and *ModelDB
type MConn interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*ModelRows, error)
	QueryRow(query string, args ...interface{}) *ModelRow
	QueryAll(proto interface{}, query string, args ...interface{}) (interface{}, error)
}

//-----------------------------------------------------------------------------
// MODELFIELD & MODELINFO

// Represents meta info about the field of a "model"
type ModelField struct {
	reflect.StructField
	Column  string
	Null    bool
	Autoinc bool
}

// Represents meta info about a model
type ModelInfo struct {
	Type           reflect.Type
	TableName      string
	Fields         []*ModelField
	FieldsSimple   string
	FieldsPrefixed string
	FieldsInsert   string
	Placeholders   string
}

// Global cache
var allModelInfos = map[string]*ModelInfo{}

func GetModelInfo(i interface{}) *ModelInfo {
	t := reflect.TypeOf(i)
	return GetModelInfoFromType(t)
}

// Call this once after each struct type declaration
func GetModelInfoFromType(modelType reflect.Type) *ModelInfo {
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	if modelType.Kind() != reflect.Struct {
		return nil
	}
	if modelType.Implements(reflect.TypeOf((*sql.Scanner)(nil)).Elem()) {
		return nil
	}

	modelName := modelType.Name()

	// Check cache
	if allModelInfos[modelName] != nil {
		return allModelInfos[modelName]
	}

	// Construct
	m := &ModelInfo{}
	allModelInfos[modelName] = m
	m.Type = modelType
	m.TableName = strings.ToLower(modelName)

	// Fields
	numFields := m.Type.NumField()
	for i := 0; i < numFields; i++ {
		field := m.Type.Field(i)
		if field.Tag.Get("db") != "" {
			column, null, autoinc := parseDBTag(field.Tag.Get("db"))
			m.Fields = append(m.Fields, &ModelField{field, column, null, autoinc})
		}
	}

	// Simple & Prefixed
	fieldNames := []string{}
	fieldInsertNames := []string{}
	ph := []string{}
	for _, field := range m.Fields {
		fieldName, _, _ := parseDBTag(field.Tag.Get("db"))
		fieldNames = append(fieldNames, fieldName)
		if !field.Autoinc {
			fieldInsertNames = append(fieldInsertNames, fieldName)
			ph = append(ph, fmt.Sprintf("$%v", len(ph)+1))
		}
	}

	m.FieldsSimple = strings.Join(fieldNames, ", ")
	m.FieldsPrefixed = m.TableName + "." + strings.Join(fieldNames, ", "+m.TableName+".")
	m.FieldsInsert = strings.Join(fieldInsertNames, ", ")
	m.Placeholders = strings.Join(ph, ", ")

	return m
}

func parseDBTag(tag string) (fieldName string, null bool, autoinc bool) {
	s := strings.Split(tag, ",")
	fieldName = s[0]
	for _, ss := range s[1:] {
		if ss == "null" {
			null = true
		}
		if ss == "autoinc" {
			autoinc = true
		}
	}
	return
}

// Split a struct value into field values, for insertion
func (m *ModelInfo) FieldValues(i interface{}) []interface{} {
	v := reflect.ValueOf(i)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Type() != m.Type {
		log.Panicf("Invalid argument for FieldValues: Type mismatch. Expected %v but got %v",
			v.Type(), m.Type)
	}

	fvs := []interface{}{}
	for _, field := range m.Fields {
		name := field.Name
		fieldValue := v.FieldByName(name)
		if field.Autoinc { //&& fieldValue.Interface() == reflect.Zero(field.Type).Interface() {
			continue
		} else if field.Null && fieldValue.Interface() == reflect.Zero(field.Type).Interface() {
			fvs = append(fvs, nil)
		} else {
			fvs = append(fvs, fieldValue.Interface())
		}
	}
	return fvs
}

//-----------------------------------------------------------------------------
// Common methods

// Expand any model structs in args into its field components, for insertion
func expandArgs(args ...interface{}) []interface{} {
	a := []interface{}{}
	for _, arg := range args {
		modelInfo := GetModelInfo(arg)
		if modelInfo == nil {
			a = append(a, arg)
		} else {
			a = append(a, modelInfo.FieldValues(arg)...)
		}
	}
	return a
}

func isScanner(i interface{}) bool {
	_, ok := i.(sql.Scanner)
	return ok
}

// Scan row result fields into dest, which can include structs.
func scanStruct(scanner RowScanner, dest ...interface{}) error {
	destValuesP := []interface{}{}
	for _, d := range dest {
		dValueP := reflect.ValueOf(d)
		dValue := dValueP.Elem()
		if dValue.Kind() != reflect.Struct || isScanner(d) {
			destValuesP = append(destValuesP, dValueP.Interface())
		} else {
			m := GetModelInfoFromType(dValue.Type())
			for _, field := range m.Fields {
				dField := dValue.FieldByName(field.Name)
				if field.Null {
					switch field.Type.Name() {
					case "string":
						ns := NullString(dField.Interface().(string))
						destValuesP = append(destValuesP, &ns)
					case "int64":
						ni := NullInt64(dField.Interface().(int64))
						destValuesP = append(destValuesP, &ni)
					default:
						panic(errors.New("Dunno how to convert nil to " + field.Type.Name()))
					}
				} else {
					destValuesP = append(destValuesP, dField.Addr().Interface())
				}
			}
		}
	}
	return scanner.Scan(destValuesP...)
}

func _Exec(c Conn, query string, args ...interface{}) (sql.Result, error) {
	return c.Exec(ConvertPH(query), expandArgs(args...)...)
}

func _QueryRow(c Conn, query string, args ...interface{}) *ModelRow {
	return &ModelRow{c.QueryRow(ConvertPH(query), expandArgs(args...)...)}
}

func _Query(c Conn, query string, args ...interface{}) (*ModelRows, error) {
	rows, err := c.Query(ConvertPH(query), expandArgs(args...)...)
	if err != nil {
		return nil, err
	}
	return &ModelRows{rows}, nil
}

func _QueryAll(c Conn, proto interface{}, query string, args ...interface{}) (interface{}, error) {
	protos := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(reflect.TypeOf(proto))), 0, 0)
	rows, err := c.Query(ConvertPH(query), expandArgs(args...)...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		protoValueP := reflect.New(reflect.TypeOf(proto))
		err := scanStruct(rows, protoValueP.Interface())
		if err != nil {
			return nil, err
		}
		protos = reflect.Append(protos, protoValueP)
	}
	return protos.Interface(), nil
}

//-----------------------------------------------------------------------------
// ModelRow

type ModelRow struct {
	Row *sql.Row
}

func (mRow *ModelRow) Scan(dest ...interface{}) error {
	return scanStruct(mRow.Row, dest...)
}

//-----------------------------------------------------------------------------
// ModelRows

type ModelRows struct {
	Rows *sql.Rows
}

func (mRows *ModelRows) Close() error {
	return mRows.Rows.Close()
}

func (mRows *ModelRows) Columns() ([]string, error) {
	return mRows.Rows.Columns()
}

func (mRows *ModelRows) Err() error {
	return mRows.Rows.Err()
}

func (mRows *ModelRows) Next() bool {
	return mRows.Rows.Next()
}

func (mRows *ModelRows) Scan(dest ...interface{}) error {
	return scanStruct(mRows.Rows, dest...)
}

//-----------------------------------------------------------------------------
// ModelDB

type ModelDB struct {
	DB *sql.DB
}

func (mDB *ModelDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return _Exec(mDB.DB, query, args...)
}

func (mDB *ModelDB) Query(query string, args ...interface{}) (*ModelRows, error) {
	return _Query(mDB.DB, query, args...)
}

func (mDB *ModelDB) QueryRow(query string, args ...interface{}) *ModelRow {
	return _QueryRow(mDB.DB, query, args...)
}

func (mDB *ModelDB) QueryAll(proto interface{}, query string, args ...interface{}) (interface{}, error) {
	return _QueryAll(mDB.DB, proto, query, args...)
}

//-----------------------------------------------------------------------------
// ModelTx

type ModelTx struct {
	Tx        *sql.Tx
	Finalized bool
}

func (mTx *ModelTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return _Exec(mTx.Tx, query, args...)
}

func (mTx *ModelTx) Query(query string, args ...interface{}) (*ModelRows, error) {
	return _Query(mTx.Tx, query, args...)
}

func (mTx *ModelTx) QueryRow(query string, args ...interface{}) *ModelRow {
	return _QueryRow(mTx.Tx, query, args...)
}

func (mTx *ModelTx) QueryAll(proto interface{}, query string, args ...interface{}) (interface{}, error) {
	return _QueryAll(mTx.Tx, proto, query, args...)
}

func (mTx *ModelTx) Rollback() error {
	mTx.Finalized = true
	return mTx.Tx.Rollback()
}

func (mTx *ModelTx) Commit() error {
	mTx.Finalized = true
	return mTx.Tx.Commit()
}

func (mTx *ModelTx) Finalize() {
	if !mTx.Finalized {
		rbErr := mTx.Tx.Rollback()
		if rbErr != nil {
			panic(rbErr)
		}
	}
}

//-----------------------------------------------------------------------------
// Convenience methods

func Exec(query string, args ...interface{}) (sql.Result, error) {
	return _Exec(GetDB(), query, args...)
}

func QueryRow(query string, args ...interface{}) *ModelRow {
	return _QueryRow(GetDB(), query, args...)
}

func Query(query string, args ...interface{}) (*ModelRows, error) {
	return _Query(GetDB(), query, args...)
}

func QueryAll(proto interface{}, query string, args ...interface{}) (interface{}, error) {
	return _QueryAll(GetDB(), proto, query, args...)
}

func Begin(level string) (*ModelTx, error) {
	tx, err := GetDB().Begin()
	if err != nil {
		return nil, err
	}
	if level == "" {
		level = "READ COMMITTED"
	}
	_, err = tx.Exec(`SET TRANSACTION ISOLATION LEVEL ` + level)
	if err != nil {
		return nil, err
	}
	return &ModelTx{tx, false}, nil
}

// Auto-retries and commits the block of code in f.
// Any panic'd errors will be returned.
func DoBegin(level string, f func(*ModelTx)) (retErr error) {
	var tries = 0
	for {
		var retry = false
		(func() {
			// Start transaction
			tx, err := Begin(level)
			if err != nil {
				retErr = err
				retry = false
				return
			}

			// If error is ERR_SERIAL_TX, then redo.
			// Otherwise just finalize the transaction.
			defer func() {
				if e := recover(); e != nil {
					if !tx.Finalized {
						tx.Tx.Rollback()
					}
					err, ok := e.(error)
					if !ok {
						err = fmt.Errorf("%v", e)
					}
					if GetErrorType(err) == ERR_SERIAL_TX {
						retry = true
						return
					}
					// Get stack trace
					buf := make([]byte, 1<<16)
					runtime.Stack(buf, false)
					log.Printf("Panic intercepted in DoBegin : %v\n%v\n", err.Error(), string(buf))
					retErr = err
					retry = false
					return
				}
				if !tx.Finalized {
					rbErr := tx.Tx.Rollback()
					if rbErr != nil {
						retErr = rbErr
						retry = false
						return
					}
				}
			}()

			// Call f
			f(tx)

			// Commit
			err = tx.Commit()
			if GetErrorType(err) == ERR_SERIAL_TX {
				retry = true
				return
			}
			if err != nil {
				retErr = err
				retry = false
				return
			}
		})()

		if retry {
			tries++
			log.Printf("Retrying serializable transaction: try %v", tries)
			continue
		} else {
			break
		}
	}
	return
}

func DoBeginSerializable(f func(*ModelTx)) (retErr error) {
	return DoBegin("SERIALIZABLE", f)
}

//-----------------------------------------------------------------------------
// Null*

type NullString string

func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		*ns = NullString("")
	} else {
		*ns = NullString(string(value.([]uint8)))
	}
	return nil
}

type NullInt64 int64

func (ni *NullInt64) Scan(value interface{}) error {
	if value == nil {
		*ni = NullInt64(0)
	} else {
		*ni = NullInt64(int64(value.(int64)))
	}
	return nil
}
