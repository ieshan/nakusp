module github.com/ieshan/nakusp/transports/redis

go 1.26

replace github.com/ieshan/nakusp => ../../

require (
	github.com/ieshan/idx v1.3.0
	github.com/ieshan/nakusp v0.0.0
	github.com/redis/go-redis/v9 v9.20.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)
