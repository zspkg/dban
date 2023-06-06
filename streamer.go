package dban

import (
	"context"
	"gitlab.com/distributed_lab/kit/pgdb"
	"gitlab.com/distributed_lab/logan/v3"
	"gitlab.com/distributed_lab/logan/v3/errors"
	"strconv"
)

const defaultBatchSize uint64 = 15

// Streamable is an interface that an object (for instance, database querier)
// must implement in order to be able to stream data
type Streamable[T any] interface {
	SelectWithPageParams(pageParams pgdb.OffsetPageParams) ([]T, error)
}

// Streamer is an interface implementing functions that allow to stream through the data
type Streamer[T any] interface {
	// Select returns a batch of entities of a size specified in StreamerInitParams and
	// with a page offset specified in function arguments
	Select(pageNumber uint64) ([]T, error)
	// FormListAndProcess forms a list according to a FormList function and applies a function
	// specified as an argument
	FormListAndProcess(fn func(ctx context.Context, t T) error) error
	// FormList returns a batch of entities and turns to the next available page (or sets it to 1 if
	// an end of a list was reached)
	FormList() ([]T, error)
	// GetCurrentPage returns a page we are at while streaming through data
	GetCurrentPage() (uint64, error)
}

// StreamerInitParams are parameters specified when initializing a new streamer
type StreamerInitParams[T any] struct {
	Stream      Streamable[T]
	KeyValueQ   KeyValueQ
	KeyValueKey string
	BatchSize   *uint64
	Log         *logan.Entry
	Ctx         *context.Context
}

// NewStreamer creates a new instance of Streamer using StreamerInitParams. All
// values are necessary except for a Log, BatchSize, and Ctx which could be omitted
// (in that case, Log wouldn't log anything, BatchSize would be set to 15 and Ctx to context.Background())
func NewStreamer[T any](initParams StreamerInitParams[T]) Streamer[T] {
	var (
		batchSize = defaultBatchSize
		ctx       = context.Background()
	)

	if initParams.BatchSize != nil {
		batchSize = *initParams.BatchSize
	}
	if initParams.Ctx != nil {
		ctx = *initParams.Ctx
	}

	return &streamer[T]{
		Stream:      initParams.Stream,
		KeyValueQ:   initParams.KeyValueQ,
		KeyValueKey: initParams.KeyValueKey,
		BatchSize:   batchSize,
		Log:         initParams.Log,
		Ctx:         ctx,
	}
}

// Streamer is a structure to stream through some querier
type streamer[T any] struct {
	Stream      Streamable[T]
	KeyValueQ   KeyValueQ
	KeyValueKey string
	BatchSize   uint64
	Log         *logan.Entry
	Ctx         context.Context
}

func (s *streamer[T]) Select(pageNumber uint64) ([]T, error) {
	return s.Stream.SelectWithPageParams(pgdb.OffsetPageParams{
		Limit:      s.BatchSize,
		PageNumber: pageNumber})
}

func (s *streamer[T]) FormListAndProcess(fn func(ctx context.Context, t T) error) error {
	entities, err := s.FormList()
	if err != nil {
		return errors.Wrap(err, "failed to form a list of entities")
	}

	for _, entity := range entities {
		if err = fn(s.Ctx, entity); err != nil {
			return errors.Wrap(err, "failed to process an entity")
		}
	}

	return nil
}

func (s *streamer[T]) FormList() ([]T, error) {
	// Get page number to begin from
	pageNumber, err := s.GetCurrentPage()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current page number")
	}

	// Select entities from the prior found page number
	entities, err := s.Select(pageNumber)
	if err != nil {
		return nil, errors.Wrap(err, "failed to select entities")
	}

	// If entities list is empty, and we are on the first page, there are no entities in the database
	if len(entities) == 0 && pageNumber == 0 {
		if s.Log != nil {
			s.Log.Warn("Entities list is empty")
		}
		return nil, nil
	}

	// If pairs list is empty, we should begin from the 1st page
	if len(entities) == 0 {
		// Setting page number to 0
		if err = s.KeyValueQ.Upsert(KeyValue{Key: s.KeyValueKey, Value: "0"}); err != nil {
			return nil, errors.Wrap(err, "failed to upsert last page")
		}

		// Restart the function with a page number equal to 0
		return s.FormList()
	}

	// If the list was not empty, just increment the page number
	if err = s.KeyValueQ.Upsert(KeyValue{
		Key:   s.KeyValueKey,
		Value: strconv.FormatUint(pageNumber+1, 10),
	}); err != nil {
		return nil, errors.Wrap(err, "failed to update last processed entities")
	}

	// Return entities list
	return entities, nil
}

// GetCurrentPage returns a page we are at while streaming through data
func (s *streamer[T]) GetCurrentPage() (uint64, error) {
	pageKV, err := s.KeyValueQ.LockingGet(s.KeyValueKey)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get current cursor value", logan.F{
			"key": s.KeyValueKey,
		})
	}

	// If we did not find a cursor, initialize it with a value of 0
	if pageKV == nil {
		pageKV = &KeyValue{
			Key:   s.KeyValueKey,
			Value: "0",
		}
	}

	page, err := strconv.ParseInt(pageKV.Value, 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse cursor", logan.F{
			"kv_cursor": pageKV.Value,
		})
	}
	if page < 0 {
		return 0, errors.From(errors.New("cursor cannot be negative"), logan.F{
			"cursor": page,
		})
	}

	return uint64(page), nil
}
