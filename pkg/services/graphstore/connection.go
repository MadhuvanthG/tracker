package graphstore

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql" // driver for mysql
	_ "github.com/jackc/pgx/v4"        // driver for postgres
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/jmoiron/sqlx"

	_ "github.com/mattn/go-sqlite3" // driver for sqlite3
)

// Constants representing the DBMS's supported with a mapping to the underlying driver names used
const (
	mysql    = "mysql"
	sqlite   = "sqlite3"
	postgres = "pgx"
)

// ResolveDriverName resolves the sql driver to use for the given dbms system
func ResolveDriverName(dbmsName string) (string, error) {
	switch dbmsName {
	case "mysql":
		return mysql, nil

	case "sqlite":
		return sqlite, nil

	case "postgres":
		return postgres, nil
	}

	return "", fmt.Errorf("%s not supported, specify one of the supported systems; mysql/postgres/sqlite", dbmsName)
}

// NewDatabaseConnection opens a connection to the database
// and returns a handle to it
func NewDatabaseConnection(dbmsName string, rwDSN string, roDSN string) (rwdb *sqlx.DB, rodb *sqlx.DB, err error) {
	driver, err := ResolveDriverName(dbmsName)
	if err != nil {
		return nil, nil, err
	}

	if len(rwDSN) > 0 {
		rwdb, err = sqlx.Open(driver, rwDSN)
		if err != nil {
			return nil, nil, err
		}
	}

	rodb = rwdb
	if len(roDSN) > 0 {
		rodb, err = sqlx.Open(driver, roDSN)
		if err != nil {
			return nil, nil, err
		}
	}

	return rwdb, rodb, nil
}
