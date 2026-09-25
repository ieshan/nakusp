module github.com/ieshan/nakusp/transports/sqlite

go 1.27

replace github.com/ieshan/nakusp => ../../

require (
	github.com/ieshan/idx v1.3.3
	github.com/ieshan/nakusp v0.0.0
	github.com/mattn/go-sqlite3 v1.14.52
)

require github.com/oklog/ulid/v2 v2.1.2 // indirect
