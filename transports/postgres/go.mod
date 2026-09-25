module github.com/ieshan/nakusp/transports/postgres

go 1.27

replace github.com/ieshan/nakusp => ../../

require (
	github.com/ieshan/idx v1.3.3
	github.com/ieshan/nakusp v0.0.0
	github.com/ieshan/timi v1.1.7
	gorm.io/driver/postgres v1.6.3
	gorm.io/gorm v1.31.2
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/mattn/go-sqlite3 v1.14.52 // indirect
	github.com/oklog/ulid/v2 v2.1.2 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/text v0.39.0 // indirect
)
