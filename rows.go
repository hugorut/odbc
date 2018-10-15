// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import "C"
import (
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/alexbrainman/odbc/api"
)

var (
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
)

type Rows struct {
	os *ODBCStmt
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	switch x := r.os.Cols[index].(type) {
	case *BindableColumn:
		return toSqlType(x.CType).scanType
	case *NonBindableColumn:
		return toSqlType(x.CType).scanType
	}
	return scanTypeUnknown
}

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	switch x := r.os.Cols[index].(type) {
	case *BindableColumn:
		return toSqlType(x.CType).name
	case *NonBindableColumn:
		return toSqlType(x.CType).name
	}
	return ""
}

type sqlType struct {
	name     string
	scanType reflect.Type
}

func toSqlType(ct api.SQLSMALLINT) sqlType {
	switch ct {
	case api.SQL_C_CHAR:
		return sqlType{
			name:     "SQL_C_CHAR",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_LONG:
		return sqlType{
			name:     "SQL_C_LONG",
			scanType: scanTypeNullInt,
		}
	case api.SQL_C_SHORT:
		return sqlType{
			name:     "SQL_C_SHORT",
			scanType: scanTypeNullInt,
		}
	case api.SQL_C_FLOAT:
		return sqlType{
			name:     "SQL_C_FLOAT",
			scanType: scanTypeNullFloat,
		}
	case api.SQL_C_DOUBLE:
		return sqlType{
			name:     "SQL_C_DOUBLE",
			scanType: scanTypeNullFloat,
		}
	case api.SQL_C_NUMERIC:
		return sqlType{
			name:     "SQL_C_NUMERIC",
			scanType: scanTypeNullInt,
		}
	case api.SQL_C_DATE:
		return sqlType{
			name:     "SQL_C_DATE",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_TIME:
		return sqlType{
			name:     "SQL_C_TIME",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_TYPE_TIMESTAMP:
		return sqlType{
			name:     "SQL_C_TYPE_TIMESTAMP",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_TIMESTAMP:
		return sqlType{
			name:     "SQL_C_TIMESTAMP",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_BINARY:
		return sqlType{
			name:     "SQL_C_BINARY",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_BIT:
		return sqlType{
			name:     "SQL_C_BIT",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_WCHAR:
		return sqlType{
			name:     "SQL_C_WCHAR",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_DEFAULT:
		return sqlType{
			name:     "SQL_C_DEFAULT",
			scanType: scanTypeRawBytes,
		}
	case api.SQL_C_SBIGINT:
		return sqlType{
			name:     "SQL_C_SBIGINT",
			scanType: scanTypeNullInt,
		}
	case api.SQL_C_UBIGINT:
		return sqlType{
			name:     "SQL_C_UBIGINT",
			scanType: scanTypeNullInt,
		}
	case api.SQL_C_GUID:
		return sqlType{
			name:     "SQL_C_GUID",
			scanType: scanTypeNullInt,
		}
	}
	return sqlType{scanType: scanTypeUnknown}
}

func (r *Rows) Columns() []string {
	names := make([]string, len(r.os.Cols))
	for i := 0; i < len(names); i++ {
		names[i] = r.os.Cols[i].Name()
	}
	return names
}

func (r *Rows) Next(dest []driver.Value) error {
	ret := api.SQLFetch(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLFetch", r.os.h)
	}
	for i := range dest {
		v, err := r.os.Cols[i].Value(r.os.h, i)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

func (r *Rows) Close() error {
	return r.os.closeByRows()
}
