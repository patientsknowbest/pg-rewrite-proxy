package proxy

import (
	"fmt"
	"net"
	"testing"

	"context"

	"github.com/jackc/pgx/v4"
)

func TestParallelLuaProxy(t *testing.T) {
	// Start a proxy locally on any available port
	ss, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	go RunProxy(ss, "localhost:5432", NewLuaQueryRewriterFactory("rewrite.lua"))

	n := 100
	done := make(chan bool)
	for i := 0; i < n; i++ {
		go func(addr string) {
			// urlExample := "postgres://username:password@localhost:5432/database_name"
			conn, err := pgx.Connect(context.Background(), "postgres://martin@" + addr + "/martin")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			var name string
			var foo string
			err = conn.QueryRow(context.Background(), "select name, foo from baz").Scan(&name, &foo)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("Got name = %v foo = %v\n", name, foo)
			done <- true
		} (ss.Addr().String())
	}
	for i := 0; i < n; i++ {
		<- done
	}
	fmt.Printf("Done all")
}