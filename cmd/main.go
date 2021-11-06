package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/patientsknowbest/pg-rewrite-proxy"
)

var options struct {
	listenAddress string
	upstream      string
	luaFile       string
	replacements  arrayFlags
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, " ")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func parseReplacementRules(args []string) (map[string]string, error) {
	res := make(map[string]string, len(args))
	for _, arg := range args {
		toks := strings.Split(arg, "/")
		if len(toks) < 2 {
			return nil, fmt.Errorf("invalid replacement rule %v", arg)
		}
		res[toks[0]] = toks[1]
	}
	return res, nil
}

func main() {
	flag.StringVar(&options.listenAddress, "listen", "0.0.0.0:6432", "Listen address")
	flag.StringVar(&options.upstream, "upstream", "127.0.0.1:5432", "Upstream postgres server")
	flag.StringVar(&options.luaFile, "luaFile", "", "LUA file containing rewrite function")
	flag.Var(&options.replacements, "r", "Replacement rules, format <old>/<new>, e.g. foo/bar")
	flag.Parse()

	var rewriterFactory proxy.QueryRewriterFactory
	var err error
	if options.luaFile != "" {
		rewriterFactory = proxy.NewLuaQueryRewriterFactory(options.luaFile)
	} else if len(options.replacements) > 0 {
		replacements, err := parseReplacementRules(options.replacements)
		if err != nil {
			log.Fatal(err)
		}
		rewriterFactory = proxy.NewStringRewriterFactory(replacements)
	} else {
		log.Fatal("no rewrite rules supplied")
	}

	ln, err := net.Listen("tcp", options.listenAddress)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s", options.listenAddress)
	defer ln.Close()
	log.Fatal(proxy.RunProxy(ln, options.upstream, rewriterFactory))
}
