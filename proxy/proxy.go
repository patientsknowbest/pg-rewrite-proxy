package proxy

import (
	"fmt"
	"github.com/jackc/pgproto3/v2"
	"github.com/sergi/go-diff/diffmatchpatch"
	"net"
)

type PgRewriteProxy struct {
	backend      *pgproto3.Backend
	frontend     *pgproto3.Frontend
	clientConn   net.Conn
	upstreamConn net.Conn
	rewriter     QueryRewriter
}

func NewPgRewriteProxy(clientConn, upstreamConn net.Conn, rewriter QueryRewriter) *PgRewriteProxy {
	return &PgRewriteProxy{
		backend:      pgproto3.NewBackend(pgproto3.NewChunkReader(clientConn), clientConn),
		frontend:     pgproto3.NewFrontend(pgproto3.NewChunkReader(upstreamConn), upstreamConn),
		rewriter:     rewriter,
		clientConn:   clientConn,
		upstreamConn: upstreamConn,
	}
}

func (p *PgRewriteProxy) Run() error {
	// Startup message is special
	var startupMessage pgproto3.FrontendMessage
	var err error
	for {
		startupMessage, err = p.backend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf("error receiving startup message: %w", err)
		}
		// We can't support SSL otherwise we can't modify queries
		_, isSsl := startupMessage.(*pgproto3.SSLRequest)
		_, isGss := startupMessage.(*pgproto3.GSSEncRequest)
		if isSsl || isGss {
			_, err = p.clientConn.Write([]byte("N"))
			if err != nil {
				return fmt.Errorf("error sending deny SSL request: %w", err)
			}
		} else {
			break
		}
	}

	// Pass on the startup message
	err = p.frontend.Send(startupMessage)
	if err != nil {
		return err
	}

	// Boot two loops, one for inbound, one for outbound
	errc := make(chan error)
	go func() {
		for {
			msg, err := p.backend.Receive()
			if err != nil {
				errc <- err
				return
			}

			if queryMsg, ok := msg.(*pgproto3.Query); ok {
				newQuery, rewriteErr := p.rewriter.RewriteQuery(queryMsg.String)
				if rewriteErr != nil {
					err := p.sendRewriteError(rewriteErr)
					if err != nil {
						errc <- err
						return
					}
				} else {
					if queryMsg.String != newQuery {
						p.sendRewriteNotice(queryMsg.String, newQuery)
					}
					queryMsg.String = newQuery
				}
			}

			if parseMsg, ok := msg.(*pgproto3.Parse); ok {
				newQuery, rewriteErr := p.rewriter.RewriteParse(parseMsg.Query)
				if rewriteErr != nil {
					err := p.sendRewriteError(rewriteErr)
					if err != nil {
						errc <- err
						return
					}
				} else {
					if parseMsg.Query != newQuery {
						p.sendRewriteNotice(parseMsg.Query, newQuery)
					}
					parseMsg.Query = newQuery
				}
			}

			// don't support rewriting parameters just yet (not sure if I need to)
			//if _, ok := msg.(*pgproto3.Bind); ok {
			//}

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
	// Wait for either loop to exit
	err = <-errc
	return err
}

/// Be nice and tell the user we rewrote their query
func (p *PgRewriteProxy) sendRewriteNotice(original, new string) error {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(original, new, false)
	detail := dmp.DiffPrettyText(diffs)
	not := &pgproto3.NoticeResponse{
		Severity: "NOTICE",
		Message:  "Query was rewritten",
		Detail:   detail,
	}
	return p.backend.Send(not)
}

/// Be nice and tell the user we failed to rewrite their query
func (p *PgRewriteProxy) sendRewriteError(rewriteErr error) error {
	not := &pgproto3.NoticeResponse{
		Severity: "WARNING",
		Message:  "Failed to rewrite query",
		Detail:   rewriteErr.Error(),
	}
	return p.backend.Send(not)
}

func (p *PgRewriteProxy) Close() error {
	err := p.clientConn.Close()
	err2 := p.upstreamConn.Close()
	if err != nil {
		return err
	} else if err2 != nil {
		return err2
	}
	return nil
}
