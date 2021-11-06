module github.com/patientsknowbest/pg-rewrite-proxy

go 1.17

require (
	github.com/jackc/pgproto3/v2 v2.1.1
	github.com/sergi/go-diff v1.2.0
	github.com/yuin/gopher-lua v0.0.0-20210529063254-f4c35e4016d9
)

require (
	github.com/jackc/chunkreader/v2 v2.0.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
)

replace github.com/jackc/pgproto3/v2 v2.1.1 => github.com/patientsknowbest/pgproto3/v2 v2.1.2-0.20211103194614-9d46cb0aecb9
