package main

import (
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
)

type RPCAPI struct {
	MyHostname   string
	clusterState *ClusterState
	kvstore      *KVStore
	clients      map[string]*rpc.Client
	clientMu     sync.Mutex
}

func NewRPCAPI(clusterState *ClusterState, kvstore *KVStore) *RPCAPI {
	envHostname := config.GetString("SEMADB_RPC_HOST", "")
	if envHostname == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to get hostname")
		}
		log.Warn().Str("hostname", hostname).Msg("host not set, using hostname")
		envHostname = hostname
	}
	rpcPort := config.GetString("SEMADB_RPC_PORT", "9898")
	envHostname = envHostname + ":" + rpcPort
	log.Debug().Str("hostname", envHostname).Msg("NewRPCAPI")
	return &RPCAPI{MyHostname: envHostname, clusterState: clusterState, kvstore: kvstore, clients: make(map[string]*rpc.Client)}
}

// ---------------------------

func (api *RPCAPI) Serve() {
	rpc.Register(api)
	rpc.HandleHTTP()
	rpcPort := config.GetString("SEMADB_RPC_PORT", "9898")
	log.Info().Str("rpcPort", rpcPort).Msg("RPCAPI.Serve")
	go http.ListenAndServe(":"+rpcPort, nil)
}

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

func (api *RPCAPI) internalRoute(remoteFn string, args *RequestArgs, reply interface{}) error {
	log.Debug().Interface("args", args).Str("host", api.MyHostname).Msg(remoteFn)
	if args.Destination != api.MyHostname {
		log.Debug().Str("destination", args.Destination).Str("host", api.MyHostname).Msg(remoteFn + ": routing")
		client, err := api.Client(args.Destination)
		if err != nil {
			return fmt.Errorf("failed to get client: %v", err)
		}
		// Make request with timeout
		rpcCall := client.Go(remoteFn, args, reply, nil)
		select {
		case <-rpcCall.Done:
			if rpcCall.Error != nil {
				return fmt.Errorf("failed to call %v: %v", remoteFn, rpcCall.Error)
			}
		case <-time.After(10 * time.Millisecond):
			return fmt.Errorf(remoteFn + " timed out")
		}
		return nil
	}
	return nil
}

// ---------------------------

// Common to all RPC requests, this trick allows us to locally call the same RPC
// endpoints and let the functions route to the right server assuming mesh
// network
type RequestArgs struct {
	Source      string
	Destination string
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
	if err := api.internalRoute("RPCAPI.Ping", &args.RequestArgs, reply); err != nil {
		return fmt.Errorf("failed to route: %v", err)
	}
	reply.Message = fmt.Sprintf("Pong from semadb %v, message: %v", api.MyHostname, args.Message)
	return nil
}

// ---------------------------

type WriteKVRequest struct {
	RequestArgs
	Key       []byte
	Value     []byte
	Commit    bool
	Handoff   []string
	Timestamp int64
}

type WriteKVResponse struct {
}

func (api *RPCAPI) WriteKV(args *WriteKVRequest, reply *WriteKVResponse) error {
	if err := api.internalRoute("RPCAPI.WriteKV", &args.RequestArgs, reply); err != nil {
		return fmt.Errorf("failed to route: %v", err)
	}
	// api.kvstore.Write(args.Key, args.Value)
	return nil
}
