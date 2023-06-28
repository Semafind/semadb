package rpcapi

import (
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/kvstore"
)

type RPCAPI struct {
	MyHostname string
	kvstore    *kvstore.KVStore
	clients    map[string]*rpc.Client
	clientMu   sync.Mutex
}

func NewRPCAPI(kvstore *kvstore.KVStore) *RPCAPI {
	envHostname := config.Cfg.RpcHost
	if envHostname == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get hostname")
		}
		log.Warn().Str("hostname", hostname).Msg("host not set, using hostname")
		envHostname = hostname
	}
	rpcPort := config.Cfg.RpcPort
	envHostname = envHostname + ":" + strconv.Itoa(rpcPort)
	log.Debug().Str("hostname", envHostname).Msg("NewRPCAPI")
	return &RPCAPI{MyHostname: envHostname, kvstore: kvstore, clients: make(map[string]*rpc.Client)}
}

// ---------------------------

func (api *RPCAPI) Serve() *http.Server {
	rpc.Register(api)
	rpc.HandleHTTP()
	server := &http.Server{
		Addr:         api.MyHostname,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		// service connections
		log.Info().Str("rpcHost", api.MyHostname).Msg("RPCAPI.Serve")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Failed to listen and serve RPCAPI")
		}
	}()
	return server
}

// func (api *RPCAPI) Close() {
// 	log.Info().Msg("RPCAPI.Close")
// 	for destination, client := range api.clients {
// 		if err := client.Close(); err != nil {
// 			log.Error().Err(err).Str("destination", destination).Msg("Failed to close RPC client")
// 		}
// 	}
// }

func (api *RPCAPI) Client(destination string) (*rpc.Client, error) {
	api.clientMu.Lock()
	defer api.clientMu.Unlock()
	if client, ok := api.clients[destination]; ok {
		return client, nil
	}
	log.Debug().Str("destination", destination).Msg("RPCAPI.Client: creating new client")
	client, err := rpc.DialHTTP("tcp", destination)
	if err != nil {
		return nil, err
	}
	api.clients[destination] = client
	return client, nil
}

func (api *RPCAPI) internalRoute(remoteFn string, args Destinationer, reply interface{}) error {
	destination := args.Destination()
	log.Debug().Str("destination", destination).Str("host", api.MyHostname).Msg(remoteFn + ": routing")
	client, err := api.Client(destination)
	if err != nil {
		return fmt.Errorf("failed to get client: %v", err)
	}
	// Make request with timeout
	rpcCall := client.Go(remoteFn, args, reply, nil)
	select {
	case <-rpcCall.Done:
		if rpcCall.Error != nil {
			return fmt.Errorf("failed to call %v: %w", remoteFn, rpcCall.Error)
		}
	case <-time.After(time.Duration(config.Cfg.RpcTimeout) * time.Millisecond):
		return fmt.Errorf(remoteFn+" timed out: %w", ErrRPCTimeout)
	}
	return nil
}

// ---------------------------

type Destinationer interface {
	Destination() string
}

// Common to all RPC requests, this trick allows us to locally call the same RPC
// endpoints and let the functions route to the right server assuming mesh
// network
type RequestArgs struct {
	Source string
	Dest   string
}

func (args RequestArgs) Destination() string {
	return args.Dest
}

// ---------------------------

type PingRequest struct {
	RequestArgs
	Message string
}

type PingResponse struct {
	Message string
}

func (api *RPCAPI) Ping(args *PingRequest, reply *PingResponse) error {
	log.Debug().Interface("args", args).Str("host", api.MyHostname).Msg("RPCAPI.Ping")
	if args.Dest != api.MyHostname {
		return api.internalRoute("RPCAPI.Ping", args, reply)
	}
	reply.Message = fmt.Sprintf("Pong from semadb %v, message: %v", api.MyHostname, args.Message)
	return nil
}
