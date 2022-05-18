pg-rewrite-proxy
================

A reverse proxy for postgres which rewrites queries.

Arbitrary rewriting is supported by supplying an LUA script to the proxy application, with 'rewriteQuery' and 'rewriteParse'
functions for rewriting the query. Note the latter is used for the [extended query protocol](https://www.postgresql.org/docs/13/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY). Rewriting query parameters supplied with Bind is not yet supported.

Failure to rewrite the query will raise a NOTICE but it will not cause an error.

All other messages than Query are passed to the upstream unmodified. SSL connections are not supported.

Binaries for linux amd64 and arm64 are available on the [releases](https://github.com/patientsknowbest/pg-rewrite-proxy/releases/latest) page. For other architectures please build from source.

## Prerequisites:
- Go 1.17 or greater

## Build from source:
```
CGO_ENABLED=0 go build -o pg-rewrite-proxy cmd/main.go
``` 

## Usage:
```
./pg-rewrite-proxy --help
```
