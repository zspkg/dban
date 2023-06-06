package dban

import (
	"database/sql"
	"embed"
	"github.com/pkg/errors"
	migrate "github.com/rubenv/sql-migrate"
	"gitlab.com/distributed_lab/logan/v3"
)

const dialect = "postgres"

//go:embed migrations/*.sql
var Migrations embed.FS

var migrations = &migrate.EmbedFileSystemMigrationSource{
	FileSystem: Migrations,
	Root:       "migrations",
}

// KeyValueMigrator is an interface that is responsible for database migrations
type KeyValueMigrator interface {
	// MigrateUp applied migrations up related to the KV storage
	MigrateUp() (int, error)
	// MigrateDown applied migrations down related to the KV storage
	MigrateDown() (int, error)
}

type kvMigrator struct {
	dbConnection *sql.DB
	log          *logan.Entry
}

// NewKVMigrator creates a new instance of KeyValueMigrator that can migrate up and down
// the key value storage
func NewKVMigrator(dbConnection *sql.DB, log *logan.Entry) KeyValueMigrator {
	return kvMigrator{dbConnection: dbConnection, log: log}
}

func (m kvMigrator) MigrateUp() (int, error) {
	applied, err := migrate.Exec(m.dbConnection, dialect, migrations, migrate.Up)
	if err != nil {
		return 0, errors.Wrap(err, "failed to execute key value migration up")
	}
	if m.log != nil {
		m.log.WithFields(logan.F{"applied": applied}).Info("key value migrations applied")
	}

	return applied, nil
}

func (m kvMigrator) MigrateDown() (int, error) {
	applied, err := migrate.Exec(m.dbConnection, dialect, migrations, migrate.Down)
	if err != nil {
		return 0, errors.Wrap(err, "failed to execute key value migration up")
	}
	if m.log != nil {
		m.log.WithFields(logan.F{"applied": applied}).Info("key value migrations applied")
	}

	return applied, nil
}
