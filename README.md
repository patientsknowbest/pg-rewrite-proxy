pg-rewrite-proxy
================

A reverse proxy for postgres which rewrites queries.

Arbitrary rewriting is supported by supplying an LUA script to the proxy application, with a single 'rewrite'
function for rewriting the query.

Failure to rewrite the query will raise a NOTICE but it will not cause an error.

All other messages than Query are passed to the upstream unmodified. SSL connections are not supported.

## Prerequisites:
- Go 1.17 or greater

## Build from source:
```
CGO_ENABLED=0 go build -o pg-rewrite-proxy main.go
``` 

## Usage:
```
./pg-rewrite-proxy --help
```
