package dban

import (
	"gitlab.com/distributed_lab/kit/pgdb"
	"gitlab.com/distributed_lab/logan/v3"
	"gitlab.com/distributed_lab/logan/v3/errors"
	"strconv"
)

type Streamable[T any] interface {
	SelectWithPageParams(pageParams pgdb.OffsetPageParams) ([]T, error)
}

type Streamer[T any] struct {
	Stream    Streamable[T]
	KeyValueQ KeyValueQ
	KeyQ      string
	BatchSize uint64
	Log       *logan.Entry
}

func (s *Streamer[T]) Select(pageNumber uint64) ([]T, error) {
	return s.Stream.SelectWithPageParams(pgdb.OffsetPageParams{
		Limit:      s.BatchSize,
		PageNumber: pageNumber})
}

func (s *Streamer[T]) FormList() ([]T, error) {
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
		s.Log.Warn("Entities list is empty")
		return nil, nil
	}

	// If pairs list is empty, we should begin from the 1st page
	if len(entities) == 0 {
		// Setting page number to 0
		if err = s.KeyValueQ.Upsert(KeyValue{Key: s.KeyQ, Value: "0"}); err != nil {
			return nil, errors.Wrap(err, "failed to upsert last page")
		}

		// Restart the function with a page number equal to 0
		return s.FormList()
	}

	// If the list was not empty, just increment the page number
	if err = s.KeyValueQ.Upsert(KeyValue{
		Key:   s.KeyQ,
		Value: strconv.FormatUint(pageNumber+1, 10),
	}); err != nil {
		return nil, errors.Wrap(err, "failed to update last processed entities")
	}

	// Return entities list
	return entities, nil
}

func (s *Streamer[T]) GetCurrentPage() (uint64, error) {
	pageKV, err := s.KeyValueQ.LockingGet(s.KeyQ)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get current cursor value", logan.F{
			"key": s.KeyQ,
		})
	}

	// If we did not find a cursor, initialize it with a value of 0
	if pageKV == nil {
		pageKV = &KeyValue{
			Key:   s.KeyQ,
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
