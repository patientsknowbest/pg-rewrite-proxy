#!/usr/bin/env sh
CGO_ENABLED=0 go build -o pg-rewrite-proxy cmd/main.go
