// Package stratus is a lazy implementation of a singleton pattern for a single
// database. The current implementation does not support multiple database.
// Will cross that bridge when needed. For now, this is sufficient. It is
// assumed that the database will be initialized from within `cmd/main.go` by
// calling `stratus.Connect`, and that the `GetInstance()` function will be used to
// load the reference into other services.
package stratus

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	cloudsqlconn "github.com/funayman/cloud-sql-go-connector"
	"github.com/funayman/cloud-sql-go-connector/postgres/pgxv5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	db *gorm.DB
)

// Connect opens the connection to the database through GORM. Will panic if a
// connection fails, will return an error if issues arise when trying to set
// DB options.
func Connect(driver, dsn string, opts ...func(*sql.DB) error) (err error) {
	cfg := &gorm.Config{Logger: logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second,  // Slow SQL threshold
			LogLevel:                  logger.Error, // Log level
			IgnoreRecordNotFoundError: true,         // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,        // Disable color
		},
	)}

	driver = strings.ToLower(driver)
	switch driver {
	case "cloudsql-postgres":
		authFile := "/etc/sql/auth.json"
		authOption := []cloudsqlconn.Option{cloudsqlconn.WithIAMAuthN()}
		// check if default location for JSON key file exists, and use it for
		// authentication if so. otherwise continue to use the application default
		// credentials. this is typically only used for deployed instances and not
		// for local development.
		if _, err := os.Stat(authFile); err == nil {
			authOption = append(authOption, cloudsqlconn.WithCredentialsFile(authFile))
		}

		_, err = pgxv5.RegisterDriver(
			"cloudsql-postgres",
			authOption...,
		)
		if err != nil {
			return fmt.Errorf("pgxv5.RegisterDriver(...): %v", err)
		}

		sdb, err := sql.Open(driver, dsn)
		if err != nil {
			return fmt.Errorf("sql.Open(...): %v", err)
		}

		db, err = gorm.Open(postgres.New(postgres.Config{Conn: sdb}), cfg)
		if err != nil {
			return fmt.Errorf("unable to open db: %w", err)
		}
	case "postgresql", "postgres":
		db, err = gorm.Open(postgres.Open(dsn), cfg)
		if err != nil {
			return fmt.Errorf("unable to open db: %w", err)
		}
	default:
		return errors.New("unsupported database: " + driver)
	}

	// support db options
	sdb, err := db.DB()
	if err != nil {
		return fmt.Errorf("unable to fetch *sql.DB from *gorm.DB: %w", err)
	}
	for _, opt := range opts {
		if err := opt(sdb); err != nil {
			return fmt.Errorf("db opts failure: %w", err)
		}
	}

	return nil
}

// GetInstance is a lazy devs attempt to provide a singleton to the primary db
// instance. GetInstance will panic if the database has not been initialized by
// calling `db.Connect`.
func GetInstance() *gorm.DB {
	if db == nil {
		panic("database accessed before initialized")
	}

	return db
}

// WithMaxConnections allows for the setting of `MaxOpenConns` for the
// underlying `*sql.DB` instance during database initialization.
func WithMaxConnections(max int) func(*sql.DB) error {
	return func(db *sql.DB) error {
		db.SetMaxOpenConns(max)
		return nil
	}
}

// WithMaxIdleConnections allows for the setting of `MaxIdleConns` for the
// underlying `*sql.DB` instance during database initialization.
func WithMaxIdleConnections(max int) func(*sql.DB) error {
	return func(db *sql.DB) error {
		db.SetMaxIdleConns(max)
		return nil
	}
}
