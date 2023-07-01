package cluster

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/kvstore"
)

func (c *ClusterNode) rpcClient(destination string) (*rpc.Client, error) {
	c.rpcClientsMu.Lock()
	defer c.rpcClientsMu.Unlock()
	if client, ok := c.rpcClients[destination]; ok {
		return client, nil
	}
	log.Debug().Str("destination", destination).Msg("Creating new rpc client")
	client, err := rpc.DialHTTP("tcp", destination)
	if err != nil {
		return nil, err
	}
	c.rpcClients[destination] = client
	return client, nil
}

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

func (c *ClusterNode) internalRoute(remoteFn string, args Destinationer, reply interface{}) error {
	destination := args.Destination()
	log.Debug().Str("destination", destination).Str("host", c.MyHostname).Msg(remoteFn + ": routing")
	client, err := c.rpcClient(destination)
	if err != nil {
		return fmt.Errorf("failed to get client: %v", err)
	}
	// Make request with timeout
	rpcCall := client.Go(remoteFn, args, reply, nil)
	select {
	case <-rpcCall.Done:
		if rpcCall.Error != nil {
			// Check if the connection is shutdown
			if rpcCall.Error == rpc.ErrShutdown {
				// Remove dead client from cache
				c.rpcClientsMu.Lock()
				delete(c.rpcClients, destination)
				c.rpcClientsMu.Unlock()
			}
			// The method's return value, if non-nil, is passed back as a string that the client sees as if created by errors.New
			// This means error wrapping, errors.Is and equality checks do not work as expected from rpcCall.Error
			// We do this ugly re-wrap to get the original error
			// TODO: organise remote errors into a manageable list
			finalErr := rpcCall.Error
			switch rpcCall.Error.Error() {
			case kvstore.ErrExistingKey.Error():
				finalErr = kvstore.ErrExistingKey
			case kvstore.ErrStaleData.Error():
				finalErr = kvstore.ErrStaleData
			case kvstore.ErrKeyNotFound.Error():
				finalErr = kvstore.ErrKeyNotFound
			}
			return fmt.Errorf("failed to call %v: %w", remoteFn, finalErr)
		}
	case <-time.After(time.Duration(config.Cfg.RpcTimeout) * time.Millisecond):
		return fmt.Errorf(remoteFn+" timed out: %w", ErrTimeout)
	}
	return nil
}
