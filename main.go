package main

import (
	"flag"
	"log"
	"net"
	"pg-rewrite-proxy/proxy"
	"strings"
)

var options struct {
	listenAddress string
	upstream      string
	//luaFile       string
	replacements arrayFlags
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	flag.StringVar(&options.listenAddress, "listen", "0.0.0.0:6432", "Listen address")
	flag.StringVar(&options.upstream, "upstream", "127.0.0.1:5432", "Upstream postgres server")
	//flag.StringVar(&options.luaFile, "luaFile", "rewrite.lua", "LUA file containing rewrite function")
	flag.Var(&options.replacements, "r", "Replacement rules, format <old>/<new>, e.g. foo/bar")
	flag.Parse()

	replacements := make(map[string]string, len(options.replacements))
	for _, r := range options.replacements {
		s := strings.Split(r, "/")
		if len(s) != 2 {
			log.Fatalf("Bad replacement rule %s", s)
		}
		replacements[s[0]] = s[1]
	}
	log.Printf("Replacement rules: %v", replacements)
	rewriter := proxy.NewStringRewriter(replacements)

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
