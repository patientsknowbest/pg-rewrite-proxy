package main

import (
	"flag"
	"fmt"
	"github.com/jackc/pgproto3/v2"
	lua "github.com/yuin/gopher-lua"
	"log"
	"net"
)

type PgRewriteProxyBackend struct {
	backend      *pgproto3.Backend
	frontend     *pgproto3.Frontend
	clientConn   net.Conn
	upstreamConn net.Conn
	rewriteFunc  func(string) (string, error)
}

func NewPgRewriteProxyBackend(clientConn, upstreamConn net.Conn, rewriteFunc func(string) (string, error)) *PgRewriteProxyBackend {
	return &PgRewriteProxyBackend{
		backend:      pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn),
		frontend:     pgproto3.NewFrontend(pgproto3.NewChunkReader(upstreamConn), upstreamConn),
		clientConn:   clientConn,
		upstreamConn: upstreamConn,
		rewriteFunc:  rewriteFunc,
	}
}

func (p *PgRewriteProxyBackend) Run() error {
	err := p.handleStartup()
	if err != nil {
		return err
	}

	// Separate goroutines for messages back & forth
	errc := make(chan error)
	go func() {
		for {
			msg, err := p.backend.Receive()
			if err != nil {
				errc <- err
				return
			}

			if q, ok := msg.(*pgproto3.Query); ok {
				log.Printf("Query is %v\n", q.String)
				newQuery, err := p.rewriteFunc(q.String)
				if err != nil {
					not := &pgproto3.NoticeResponse{
						Severity: "WARNING",
						Message:  "Failed to rewrite query",
						Detail:   err.Error(),
					}
					err = p.backend.Send(not)
					if err != nil {
						errc <- err
						return
					}
				} else {
					log.Printf("Rewritten to %v\n", newQuery)
					q.String = newQuery
				}
			}

			err = p.frontend.Send(msg)
			if err != nil {
				errc <- err
				return
			}
		}
	}()
	go func() {
		for {
			for {
				bm, err := p.frontend.Receive()
				if err != nil {
					errc <- err
					return
				}
				err = p.backend.Send(bm)
				if err != nil {
					errc <- err
					return
				}
			}
		}
	}()
	err = <-errc
	return err
}

func (p *PgRewriteProxyBackend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	if _, ok := startupMessage.(*pgproto3.SSLRequest); ok {
		_, err = p.clientConn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	} else {
		err = p.frontend.Send(startupMessage)
		if err != nil {
			return err
		}
		for {
			bm, err := p.frontend.Receive()
			if err != nil {
				return err
			}
			err = p.backend.Send(bm)
			if err != nil {
				return err
			}
			if _, ok := bm.(*pgproto3.ReadyForQuery); ok {
				break
			}
		}
	}
	return nil
}

func (p *PgRewriteProxyBackend) Close() error {
	err := p.clientConn.Close()
	err2 := p.upstreamConn.Close()
	if err != nil {
		return err
	} else if err2 != nil {
		return err2
	}
	return nil
}

var options struct {
	listenAddress string
	upstream      string
	luaFile       string
}

func main() {
	flag.StringVar(&options.listenAddress, "listen", "0.0.0.0:5432", "Listen address")
	flag.StringVar(&options.upstream, "upstream", "0.0.0.0:45432", "Upstream postgres server")
	flag.StringVar(&options.luaFile, "luaFile", "rewrite.lua", "LUA file containing rewrite function")
	flag.Parse()

	// Setup lua interpreter
	l := lua.NewState()
	defer l.Close()
	if err := l.DoFile(options.luaFile); err != nil {
		log.Fatal(err)
	}

	rfn := l.GetGlobal("rewrite")
	if _, ok := rfn.(*lua.LFunction); !ok {
		log.Fatalf("Unable to find rewrite function in lua file %s\n", options.luaFile)
	}

	rewritefn := func(input string) (string, error) {
		err := l.CallByParam(lua.P{
			Fn:      rfn,
			NRet:    1,
			Protect: true,
		}, lua.LString(input))
		if err != nil {
			return input, err
		}
		// Get the returned value from the stack and cast it to a lua.LString
		rValue := l.Get(-1)
		defer l.Pop(1)
		if str, ok := rValue.(lua.LString); ok {
			return string(str), nil
		} else {
			return input, fmt.Errorf("Incorrect return type from rewrite function %s", rValue.Type())
		}
	}

	_, err := rewritefn("foo")
	if err != nil {
		log.Fatal(err)
	}

	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s", options.listenAddress)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		upstreamConn, err := net.Dial("tcp", options.upstream)
		if err == nil {
			b := NewPgRewriteProxyBackend(conn, upstreamConn, rewritefn)
			go func() {
				defer b.Close()
				err := b.Run()
				if err != nil {
					log.Println(err)
				}
				log.Println("Closed connection from", conn.RemoteAddr())
			}()
		} else {
			log.Println(err)
		}
	}
}
