package cluster

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	c.logger.Info().Int("contactedServers", len(postage)).Msg("finished user entry sync")
	return nil
}

const CHUNKSIZE = 8 * 1024 * 1024 // 8MB

func (c *ClusterNode) sendShardFile(destination string, path string) error {
	c.logger.Debug().Str("path", path).Str("destination", destination).Msg("sending shard")
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open shard file: %w", err)
	}
	defer f.Close()
	// ---------------------------
	// Send the file in chunks
	startTime := time.Now()
	buf := make([]byte, CHUNKSIZE)
	shardDir := filepath.Dir(path)
	shardId := filepath.Base(shardDir)
	colDir := filepath.Dir(shardDir)
	colId := filepath.Base(colDir)
	userDir := filepath.Dir(colDir)
	userId := filepath.Base(userDir)
	for i := 0; ; i++ {
		// At the end of a file, read returns 0 bytes and io.EOF
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read shard file: %w", err)
		}
		req := RPCSendShardRequest{
			RPCRequestArgs: RPCRequestArgs{
				Source: c.MyHostname,
				Dest:   destination,
			},
			ShardId:      shardId,
			CollectionId: colId,
			UserId:       userId,
			ChunkIndex:   i,
			ChunkData:    buf[:n],
		}
		rpcResp := RPCSendShardResponse{}
		if err := c.RPCSendShard(&req, &rpcResp); err != nil {
			return fmt.Errorf("failed to send shard file chunk: %w", err)
		}
		if rpcResp.BytesWritten != n {
			return fmt.Errorf("failed to send all shard file chunk: %w", err)
		}
		if err == io.EOF {
			f.Close()
			// Time to check if everything went well
			checksum, err := FileHash(path)
			if err != nil {
				return fmt.Errorf("failed to compute checksum of shard file: %w", err)
			}
			if checksum != rpcResp.Checksum {
				return fmt.Errorf("checksum mismatch after sending shard file: %w", err)
			}
			// Good, good so far. We know we have the file safely
			// delivered, we can now clean up the shard directory.
			dirPath := filepath.Dir(path)
			if err := os.RemoveAll(dirPath); err != nil {
				return fmt.Errorf("failed to remove shard directory after sync: %w", err)
			}
			// Delete parent directories if they are empty
			dirPath = filepath.Dir(dirPath)
			for {
				// This will error if the directory is not empty
				if err := os.Remove(dirPath); err != nil {
					break
				}
				// Delete up the directory tree
				dirPath = filepath.Dir(dirPath)
			}
			// We're done
			break
		}
	}
	c.logger.Debug().Dur("duration", time.Since(startTime)).Str("path", path).Str("destination", destination).Msg("sent shard")
	return nil
}

func (c *ClusterNode) syncShards() error {
	// This sync function assumes the rest of the services are not running, so
	// we have full control over the shards. Otherwise, we must consult the
	// shard manager to block any opening of shards etc while we are syncing.
	// ---------------------------
	// Shards are stored by the shard manager currently under a path like
	// rootDir/userCollections/userId/collectionId/shardId/sharddb.bbolt
	// e.g. /data/userCollections/alice/mycollection/00a9e56b-c882-4991-babf-29aacf6fc681/sharddb.bbolt
	// ---------------------------
	c.logger.Info().Msg("starting shard sync")
	// ---------------------------
	shardDir := filepath.Join(c.cfg.ShardManager.RootDir, USERCOLSDIR) // This would be /data/userCollections
	shardPaths := make([]string, 0)
	filepath.Walk(shardDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("failed to walk shard directory: %w", err)
		}
		if filepath.Base(path) == "sharddb.bbolt" {
			shardPaths = append(shardPaths, path)
		}
		return nil
	})
	// ---------------------------
	// Destination to a list of shard paths
	postage := make(map[string][]string, len(c.Servers))
	// ---------------------------
	c.logger.Debug().Int("count", len(shardPaths)).Msg("found shards to sync")
	for _, path := range shardPaths {
		shardId := filepath.Base(filepath.Dir(path)) // the UUID of the shard
		destination := RendezvousHash(shardId, c.Servers, 1)[0]
		if destination == c.MyHostname {
			continue
		}
		// This shard needs to go, be gone shard!
		postage[destination] = append(postage[destination], path)
	}
	// ---------------------------
	// Time to create some traffic, we'll create one worker per destination to
	// not overload the network or the target server.
	var wg sync.WaitGroup
	errs := make(chan error, len(postage))
	for dest, paths := range postage {
		wg.Add(1)
		go func(dest string, paths []string) {
			defer wg.Done()
			for _, path := range paths {
				if err := c.sendShardFile(dest, path); err != nil {
					errs <- err
					return
				}
			}
		}(dest, paths)
	}
	go func() {
		wg.Wait()
		close(errs)
	}()
	// Check for errors
	for err := range errs {
		if err != nil {
			return fmt.Errorf("failed to send shards: %w", err)
		}
	}
	// ---------------------------
	c.logger.Info().Int("contactedServers", len(postage)).Msg("finished shard sync")
	return nil
}

func (c *ClusterNode) Sync() error {
	if len(c.Servers) == 1 && c.Servers[0] == c.MyHostname {
		// We are the only server, so no need to sync anything.
		c.logger.Info().Msg("no other servers to sync user entries with")
		return nil
	}
	c.logger.Info().Strs("servers", c.Servers).Str("myhostname", c.MyHostname).Msg("syncing cluster node state")
	if err := c.syncUserCollections(); err != nil {
		return fmt.Errorf("failed to sync user collections: %w", err)
	}
	if err := c.syncShards(); err != nil {
		return fmt.Errorf("failed to sync shards: %w", err)
	}
	return nil
}
