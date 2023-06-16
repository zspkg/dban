# dban
[![Go Reference](https://pkg.go.dev/badge/github.com/zspkg/dban.svg)](https://pkg.go.dev/github.com/zspkg/dban)
[![Go Report Card](https://goreportcard.com/badge/github.com/zspkg/dban)](https://goreportcard.com/report/github.com/zspkg/dban)

Helpful tools for working with the `postgres` database. Currently, `dban` includes:

- a key value storage that can store and retrieve strings from the tables;
- a streamer that is convenient when one wants to make runners that select a batch of entities from the table and processes them;

# How to install?
Simply run
```bash
go install github.com/zspkg/dban
```

# How to use?
## Key Value Storage
**Step 1.** Add a migration from `example-migration/00x_key_value.sql` to your list of migrations.

**Step 2.** You might use key value in the following way, for instance:
```go
import "github.com/zspkg/dban"

type Foo struct {
	kvQ dban.KeyValueQ
}

func NewFoo(db *pgdb.DB) Foo {
	return Foo{kvQ: dban.NewKeyValueQ(db)}
}

func (f Foo) GetBar() string {
	bar := f.kvQ.MustGet("bar")
	return bar.Value
}

func (f Foo) PutBuzz() error {
	return f.kvQ.Upsert(dban.KeyValue{Key: "buzz", Value: "buzzzzz"})
}
```

## Streamer

The code below implements a processor that takes 15 `Foo` objects from `FooQ` and processes each of them using `FooProcessor->ProcessFoo` function. 

```go
type Foo struct{}

type FooQ interface {
	SelectWithPageParams(pageParams pgdb.OffsetPageParams) ([]Foo, error)
}

type FooProcessor struct {
	streamer dban.Streamer[Foo]
	log      *logan.Entry
}

func NewFooProcessor(cfg config.Config) FooProcessor {
	var (
		batchSize = uint64(15)
		log       = cfg.Log()
	)
	return FooProcessor{
		streamer: dban.NewStreamer(dban.StreamerInitParams[Foo]{
			Stream:      NewFooQ(),
			KeyValueQ:   dban.NewKeyValueQ(cfg.DB()),
			KeyValueKey: "foo-processor",
			BatchSize:   &batchSize,
			Log:         log,
		}),
	}
}

func (p FooProcessor) ProcessFoo(_ context.Context, _ Foo) error {
	fmt.Println("Hey, I processed this foo!")
	return nil
}

func (p FooProcessor) Run(ctx context.Context) error {
	running.WithBackOff(ctx,
		p.log,
		"foo-processor",
		func(ctx context.Context) error {
			return p.streamer.FormListAndProcess(p.ProcessFoo)
		},
		time.Second,
		time.Second,
		time.Second,
	)

	return nil
}
```
