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
}

type PingResponse struct {
	Message string
}

func (api *RPCAPI) Ping(args *PingRequest, reply *PingResponse) error {
	log.Debug().Interface("args", args).Str("host", api.MyHostname).Msg("RPCAPI.Ping")
	if args.Destination != api.MyHostname {
		log.Debug().Str("destination", args.Destination).Str("host", api.MyHostname).Msg("RPCAPI.Ping: routing")
		client, err := api.Client(args.Destination)
		if err != nil {
			return fmt.Errorf("failed to get client: %v", err)
		}
		// Make request with timeout
		rpcCall := client.Go("RPCAPI.Ping", args, reply, nil)
		select {
		case <-rpcCall.Done:
			if rpcCall.Error != nil {
				return fmt.Errorf("failed to call RPCAPI.Ping: %v", rpcCall.Error)
			}
		case <-time.After(10 * time.Millisecond):
			return fmt.Errorf("RPCAPI.Ping timed out")
		}
		return nil
	}
	reply.Message = "pong from semadb " + api.MyHostname
	return nil
}

// ---------------------------
