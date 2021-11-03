package proxy

import (
	"fmt"
	lua "github.com/yuin/gopher-lua"
	"strings"
)

// Generic query rewriter interface
type QueryRewriter interface {
	RewriteQuery(string) (string, error)
	RewriteParse(string) (string, error)
}

// Dumb String replacement implementation
type StringRewriter struct {
	replacements map[string]string
}

func NewStringRewriter(replacements map[string]string) *StringRewriter {
	return &StringRewriter{replacements: replacements}
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
	// TODO: MFA - LState isn't safe to share between goroutines, this needs rework.
	l *lua.LState
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

func (r *LuaQueryRewriter) RewriteQuery(query string) (string, error) {
	return r.rewriteInternal(query, "rewriteQuery")
}

func (r *LuaQueryRewriter) RewriteParse(query string) (string, error) {
	return r.rewriteInternal(query, "rewriteParse")
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
	return &LuaQueryRewriter{
		l: l,
	}, nil
}
