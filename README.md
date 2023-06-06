# dban

Helpful tools for working with the `postgres` database. Currently, `dban` includes:

- a key value storage that can store and retrieve strings from the tables;
- a streamer that is convenient when one wants to make runners that select a batch of entities from the table and processes them;