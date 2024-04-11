/*
This package provides msgpack encoding around the net/rpc package. It follows
the original code net/rpc closely just replacing the codec part with msgpack.

Why msgpack? Because it is the internal encoding used by the database and it
makes it safer for us to know that the data we encode on disk will be encoded in
the same way for transfering data between nodes.
*/
package mrpc

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/rs/zerolog/log"
)

const connected = "200 Connected to Go RPC"

// DialHTTP connects to an HTTP RPC server at the specified network address
// listening on the default HTTP RPC path.
// Source credit: net/rpc package
func DialHTTP(network, address string) (*rpc.Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n")

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		/* HERE: the codec is swapped out for msgpack. We create this
		 * scaffholding just to be able to call using custom codec. */
		codec := NewMsgpackCodec(conn)
		return rpc.NewClientWithCodec(codec), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, &net.OpError{
		Op:   "dial-http",
		Net:  network + " " + address,
		Addr: nil,
		Err:  err,
	}
}

func NewHTTPServer(addr string, rpcServer *rpc.Server) *http.Server {
	rpcHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "CONNECT" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusMethodNotAllowed)
			io.WriteString(w, "405 must CONNECT\n")
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			log.Error().Str("remoteAddr", r.RemoteAddr).Err(err).Msg("rpc hijacking")
			return
		}
		io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
		codec := NewMsgpackCodec(conn)
		rpcServer.ServeCodec(codec)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(rpc.DefaultRPCPath, rpcHandler)
	return &http.Server{
		Addr:         addr,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Handler:      mux,
	}
}
