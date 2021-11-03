package main

import (
	"flag"
	"log"
	"net"
	"pg-rewrite-proxy/proxy"
)

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

	rewriter, err := proxy.NewLuaQueryRewriter(options.luaFile)
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
			b := proxy.NewPgRewriteProxy(conn, upstreamConn, rewriter)
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
