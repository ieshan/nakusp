module github.com/ieshan/nakusp/transports/sqlite

go 1.26

replace github.com/ieshan/nakusp => ../../

require (
	github.com/ieshan/idx v1.3.0
	github.com/ieshan/nakusp v0.0.0
	github.com/mattn/go-sqlite3 v1.14.45
)

require github.com/oklog/ulid/v2 v2.1.1 // indirect
