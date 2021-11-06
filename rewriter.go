package proxy

import (
	"fmt"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

/// Rewriters will be constructed per goroutine, as some of them may have state that isn't safe to share
type QueryRewriterFactory interface {
	Create() (QueryRewriter, error)
}

// Generic query rewriter interface
type QueryRewriter interface {
	RewriteQuery(string) (string, error)
	RewriteParse(string) (string, error)
}

// Dumb String replacement implementation
type StringRewriter struct {
	replacements map[string]string
}

type StringRewriterFactory struct {
	replacements map[string]string
}

func NewStringRewriterFactory(replacments map[string]string) *StringRewriterFactory {
	return &StringRewriterFactory{
		replacements: replacments,
	}
}

func (r *StringRewriterFactory) Create() (QueryRewriter, error) {
	return &StringRewriter{replacements: r.replacements}, nil
}

func (r *StringRewriter) RewriteQuery(query string) (string, error) {
	return r.rewriteInternal(query)
}

func (r *StringRewriter) RewriteParse(query string) (string, error) {
	return r.rewriteInternal(query)
}

func (r *StringRewriter) rewriteInternal(query string) (string, error) {
	for k, v := range r.replacements {
		query = strings.ReplaceAll(query, k, v)
	}
	return query, nil
}

// LUA interpreter implementation
type LuaQueryRewriter struct {
	// It's not safe to share this between goroutines
	l *lua.LState
}

type LuaQueryRewriterFactory struct {
	luaFile string
}

func NewLuaQueryRewriterFactory(luaFile string) QueryRewriterFactory {
	return &LuaQueryRewriterFactory{
		luaFile: luaFile,
	}
}

func (r *LuaQueryRewriterFactory) Create() (QueryRewriter, error) {
	l := lua.NewState()
	if err := l.DoFile(r.luaFile); err != nil {
		return nil, err
	}
	return &LuaQueryRewriter{
		l: l,
	}, nil
}

func (r *LuaQueryRewriter) RewriteQuery(query string) (string, error) {
	return r.rewriteInternal(query, "rewriteQuery")
}

func (r *LuaQueryRewriter) rewriteInternal(input, function string) (string, error) {
	fn, ok := r.l.GetGlobal(function).(*lua.LFunction)
	if !ok {
		return input, fmt.Errorf("Unable to find %s function!", function)
	}
	err := r.l.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, lua.LString(input))
	if err != nil {
		return input, err
	}
	rValue := r.l.Get(-1)
	defer r.l.Pop(1)
	if str, ok := rValue.(lua.LString); ok {
		return string(str), nil
	} else {
		return input, fmt.Errorf("Incorrect return type from rewrite function %s", rValue.Type())
	}
}

func (r *LuaQueryRewriter) RewriteParse(query string) (string, error) {
	return r.rewriteInternal(query, "rewriteParse")
}

func (r *LuaQueryRewriter) Close() error {
	r.l.Close()
	return nil
}
