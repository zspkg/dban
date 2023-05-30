package dban

import (
	"database/sql"
	"github.com/Masterminds/squirrel"
	"github.com/fatih/structs"
	"gitlab.com/distributed_lab/kit/pgdb"
)

type KeyValue struct {
	Key   string `db:"key" structs:"key"`
	Value string `db:"value" structs:"value"`
}

//go:generate mockery --case=underscore --name=KeyValueQ
type KeyValueQ interface {
	New() KeyValueQ

	Get(key string) (*KeyValue, error)
	// Upsert updates value if there is one, insert if no
	Upsert(KeyValue) error
	// LockingGet reads row and locks the row for reading and updating
	// until the end of the current transaction
	LockingGet(key string) (*KeyValue, error)
}

const (
	keyValueTable = "key_value"

	keyColumn   = "key"
	valueColumn = "value"
)

var keyValueSelect = squirrel.Select("*").From(keyValueTable)

type keyValueQ struct {
	db *pgdb.DB
}

func NewKeyValueQ(db *pgdb.DB) KeyValueQ {
	return &keyValueQ{
		db: db,
	}
}

func (q *keyValueQ) Upsert(kv KeyValue) error {
	query := squirrel.Insert(keyValueTable).
		SetMap(structs.Map(kv)).
		Suffix("ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")

	return q.db.Exec(query)
}

func (q *keyValueQ) New() KeyValueQ {
	return NewKeyValueQ(q.db.Clone())
}

func (q *keyValueQ) Get(key string) (*KeyValue, error) {
	return q.get(key, false)
}

func (q *keyValueQ) LockingGet(key string) (*KeyValue, error) {
	return q.get(key, true)
}

func (q *keyValueQ) get(key string, forUpdate bool) (*KeyValue, error) {
	statement := keyValueSelect.Where(squirrel.Eq{keyColumn: key})
	if forUpdate {
		statement = statement.Suffix("FOR UPDATE")
	}

	var value KeyValue
	err := q.db.Get(&value, statement)
	if err == sql.ErrNoRows {
		return nil, nil
	}

	return &value, err
}