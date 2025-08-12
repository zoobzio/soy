module github.com/zoobzio/cereal/providers/postgres

go 1.23.1

require (
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.10.9
	github.com/zoobzio/astql v0.0.0
	github.com/zoobzio/cereal v0.0.0
	github.com/zoobzio/sentinel v0.0.0-00010101000000-000000000000
)

require (
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/zoobzio/pipz v0.6.0 // indirect
	github.com/zoobzio/zlog v0.0.0 // indirect
	golang.org/x/time v0.12.0 // indirect
)

replace github.com/zoobzio/astql => ../../../astql

replace github.com/zoobzio/cereal => ../..

replace github.com/zoobzio/sentinel => ../../../sentinel

replace github.com/zoobzio/zlog => ../../../zlog

replace github.com/zoobzio/pipz => ../../../pipz
