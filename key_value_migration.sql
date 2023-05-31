-- +migrate Up

create table key_value
(
    key   varchar(64) unique not null,
    value varchar(64)        not null
);

-- +migrate Down

drop table key_value;