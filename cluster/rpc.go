package cluster

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/semafind/semadb/config"
)

func (c *ClusterNode) rpcClient(destination string) (*rpc.Client, error) {
	c.rpcClientsMu.Lock()
	defer c.rpcClientsMu.Unlock()
	if client, ok := c.rpcClients[destination]; ok {
		return client, nil
	}
	c.logger.Debug().Str("destination", destination).Msg("Creating new rpc client")
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
type RPCRequestArgs struct {
	Source string
	Dest   string
}

func (args RPCRequestArgs) Destination() string {
	return args.Dest
}

func (c *ClusterNode) internalRoute(remoteFn string, args Destinationer, reply interface{}) error {
	destination := args.Destination()
	c.logger.Debug().Str("destination", destination).Msg(remoteFn + ": routing")
	for i := 0; i < config.Cfg.RpcRetries; i++ {
		client, err := c.rpcClient(destination)
		if err != nil {
			return fmt.Errorf("failed to get client: %v", err)
		}
		// Make request with timeout
		rpcCall := client.Go(remoteFn, args, reply, nil)
		timeout := time.NewTimer(time.Duration(config.Cfg.RpcTimeout) * time.Second)
		defer timeout.Stop()
		select {
		case <-rpcCall.Done:
			if rpcCall.Error != nil {
				// Check if the connection is shutdown
				if rpcCall.Error == rpc.ErrShutdown {
					// Remove dead client from cache
					c.rpcClientsMu.Lock()
					delete(c.rpcClients, destination)
					c.rpcClientsMu.Unlock()
					c.logger.Debug().Str("destination", destination).Msg("Removed dead client")
					continue
				}
				// The method's return value, if non-nil, is passed back as a string that the client sees as if created by errors.New
				// This means error wrapping, errors.Is and equality checks do not work as expected from rpcCall.Error
				// We try to avoid relying on returning internal errors from remote calls.
				finalErr := rpcCall.Error
				// Otherwise, we need to check the error string using an ugly switch statement below.
				// switch rpcCall.Error.Error() {
				// case ErrExists.Error():
				// 	finalErr = ErrExists
				// case ErrNotFound.Error():
				// 	finalErr = ErrNotFound
				// }
				return fmt.Errorf("failed to call %v: %w", remoteFn, finalErr)
			}
			return nil
		case <-timeout.C:
			return fmt.Errorf(remoteFn+" timed out: %w", ErrTimeout)
		}
	}
	return nil
}
