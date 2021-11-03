package proxy

import (
	"fmt"
	lua "github.com/yuin/gopher-lua"
)

// Generic query rewriter interface
type QueryRewriter interface {
	RewriteQuery(string) (string, error)
	RewriteParse(string) (string, error)
}

// LUA interpreter implementation
type LuaQueryRewriter struct {
	l            *lua.LState
	rewriteQuery *lua.LFunction
	rewriteParse *lua.LFunction
}

func (r *LuaQueryRewriter) rewriteInternal(input string, fn *lua.LFunction) (string, error) {
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

func (r *LuaQueryRewriter) RewriteQuery(query string) (string, error) {
	return r.rewriteInternal(query, r.rewriteQuery)
}

func (r *LuaQueryRewriter) RewriteParse(query string) (string, error) {
	return r.rewriteInternal(query, r.rewriteParse)
}

func (r *LuaQueryRewriter) Close() error {
	r.l.Close()
	return nil
}

func NewLuaQueryRewriter(luaFile string) (*LuaQueryRewriter, error) {
	l := lua.NewState()
	if err := l.DoFile(luaFile); err != nil {
		return nil, err
	}
	rewriteQuery, ok := l.GetGlobal("rewriteQuery").(*lua.LFunction)
	if !ok {
		return nil, fmt.Errorf("Unable to find rewriteQuery function in lua file %s\n", luaFile)
	}
	rewriteParse := l.GetGlobal("rewriteParse").(*lua.LFunction)
	if !ok {
		return nil, fmt.Errorf("Unable to find rewriteParse function in lua file %s\n", luaFile)
	}
	return &LuaQueryRewriter{
		l:            l,
		rewriteQuery: rewriteQuery,
		rewriteParse: rewriteParse,
	}, nil
}
