package cluster

import (
	"fmt"
	"strings"
	"sync"

	"github.com/semafind/semadb/diskstore"
)

/* The goal of this sync business is to re-distribute items after config changes,
 * that is when the servers change or when data needs to be routed elsewhere. In
 * this case, we need to re-evaluate what the current node has and where, if any,
 * things should go. Things in this context means user collections and shards. */

func (c *ClusterNode) syncUserCollections() error {
	// We go through all the collections we have stored and see if any should be
	// on other servers.
	// ---------------------------
	c.logger.Info().Msg("starting user entry sync")
	if len(c.Servers) == 1 && c.Servers[0] == c.MyHostname {
		// We are the only server, so no need to sync anything.
		c.logger.Info().Msg("no other servers to sync user entries with")
		return nil
	}
	// ---------------------------
	postage := make(map[string]RPCSetNodeKeyValueRequest, len(c.Servers))
	// ---------------------------
	err := c.nodedb.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(USERCOLSBUCKETKEY)
		if err != nil {
			return fmt.Errorf("failed to get user collections bucket: %w", err)
		}
		// ---------------------------
		err = b.ForEach(func(k, v []byte) error {
			// Extract userId from key, e.g. userIdDBDELIMITERcollectionId, we
			// need because routing is done on userIds
			userId := strings.Split(string(k), DBDELIMITER)[0]
			destination := RendezvousHash(userId, c.Servers, 1)[0]
			if destination != c.MyHostname {
				// This collection should be on another server, so we need to
				// send it there.
				if _, ok := postage[destination]; !ok {
					postage[destination] = RPCSetNodeKeyValueRequest{
						RPCRequestArgs: RPCRequestArgs{
							Source: c.MyHostname,
							Dest:   destination,
						},
						KeyValues: make(map[string][]byte),
						Bucket:    USERCOLSBUCKETKEY,
					}
				}
				postage[destination].KeyValues[string(k)] = v
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to iterate over user collections: %w", err)
		}
		// ---------------------------
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read user collections to sync: %w", err)
	}
	// ---------------------------
	for dest, req := range postage {
		c.logger.Info().Int("count", len(req.KeyValues)).Str("dest", dest).Msg("user collections to send")
	}
	// ---------------------------
	// We need to do this sending business after we have read otherwise we might
	// lock the database in case another server wants to send to us. There would
	// only be a loop in the system if the configurations of the servers are
	// inconsistent, i.e. server A thinks B should have something and B thinks A
	// should have it which is why we are doing the sync only one round.
	// ---------------------------
	var wg sync.WaitGroup
	errs := make(chan error, len(postage))
	for dest, req := range postage {
		wg.Add(1)
		go func(dest string, req RPCSetNodeKeyValueRequest) {
			defer wg.Done()
			c.logger.Debug().Int("count", len(req.KeyValues)).Str("dest", dest).Msg("sending user collections")
			rpcResp := RPCSetNodeKeyValueResponse{}
			if err := c.RPCSetNodeKeyValue(&req, &rpcResp); err != nil {
				errs <- fmt.Errorf("failed to send user collections to %s: %w", dest, err)
				return
			}
			if rpcResp.Count != len(req.KeyValues) {
				errs <- fmt.Errorf("failed to send all user collections to %s: %w", dest, err)
				return
			}
			// Now clean up the user collections we have sent. They live happily
			// ever after on their new server.
			err := c.nodedb.Write(func(bm diskstore.BucketManager) error {
				b, err := bm.Get(USERCOLSBUCKETKEY)
				if err != nil {
					return fmt.Errorf("failed to get user collections bucket: %w", err)
				}
				// ---------------------------
				for k := range req.KeyValues {
					if err := b.Delete([]byte(k)); err != nil {
						return fmt.Errorf("failed to delete user collection after sync: %w", err)
					}
				}
				return nil
			})
			errs <- err
		}(dest, req)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	// Check for errors
	for err := range errs {
		if err != nil {
			return fmt.Errorf("failed to send user collections: %w", err)
		}
	}
	// ---------------------------
	c.logger.Info().Int("postageCount", len(postage)).Msg("finished user entry sync")
	return nil
}

func (c *ClusterNode) Sync() error {
	c.logger.Info().Strs("servers", c.Servers).Str("myhostname", c.MyHostname).Msg("syncing cluster node state")
	return c.syncUserCollections()
}
