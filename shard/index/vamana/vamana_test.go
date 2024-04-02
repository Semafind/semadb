package vamana

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/require"
)

var vamanaParams = models.IndexVectorVamanaParameters{
	VectorSize:     2,
	DistanceMetric: "euclidean",
	SearchSize:     75,
	DegreeBound:    64,
	Alpha:          1.2,
}

func checkConnectivity(t *testing.T, nodeStore *cache.ItemCache[*graphNode], expected int) {
	visited := make(map[uint64]struct{})
	queue := make([]uint64, 0)
	queue = append(queue, STARTID)
	for len(queue) > 0 {
		nodeId := queue[0]
		queue = queue[1:]
		if _, ok := visited[nodeId]; ok {
			continue
		}
		visited[nodeId] = struct{}{}
		node, err := nodeStore.Get(nodeId)
		require.NoError(t, err)
		queue = append(queue, node.edges...)
	}
	// -1 for the start node
	require.Equal(t, expected, len(visited)-1)
}

func randPoints(size int, offset int) []IndexVectorChange {
	points := make([]IndexVectorChange, size)
	for i := 0; i < size; i++ {
		randVector := make([]float32, 2)
		randVector[0] = rand.Float32()
		randVector[1] = rand.Float32()
		points[i] = IndexVectorChange{
			// 0 is not allowed, 1 is start node
			Id:     uint64(i + offset + 2),
			Vector: randVector,
		}
	}
	return points
}

func Test_Insert(t *testing.T) {
	for _, size := range []int{1, 100, 4242} {
		t.Run(fmt.Sprintf("Size=%d", size), func(t *testing.T) {
			inv, err := NewIndexVamana("test", vamanaParams, diskstore.NewMemBucket(false))
			require.NoError(t, err)
			ctx := context.Background()
			in := utils.ProduceWithContext(ctx, randPoints(size, 0))
			errC := inv.InsertUpdateDelete(ctx, in)
			require.NoError(t, <-errC)
			checkConnectivity(t, inv.nodeStore, size)
		})
	}
}

func Test_InvalidIdInsert(t *testing.T) {
	inv, err := NewIndexVamana("test", vamanaParams, diskstore.NewMemBucket(false))
	require.NoError(t, err)
	// ---------------------------
	// Insert invalid id
	ctx := context.Background()
	in := utils.ProduceWithContext(ctx, []IndexVectorChange{{Id: 0, Vector: nil}})
	errC := inv.InsertUpdateDelete(ctx, in)
	require.Error(t, <-errC)
	// ---------------------------
	in = utils.ProduceWithContext(ctx, []IndexVectorChange{{Id: 1, Vector: nil}})
	errC = inv.InsertUpdateDelete(ctx, in)
	require.Error(t, <-errC)
}

func Test_ConcurrentCUD(t *testing.T) {
	inv, err := NewIndexVamana("test", vamanaParams, diskstore.NewMemBucket(false))
	require.NoError(t, err)
	// Pre-insert
	in := make(chan IndexVectorChange)
	errC := inv.InsertUpdateDelete(context.Background(), in)
	for _, rp := range randPoints(50, 0) {
		in <- rp
	}
	// ---------------------------
	var wg sync.WaitGroup
	wg.Add(3)
	// Insert more
	go func() {
		for _, rp := range randPoints(50, 50) {
			in <- rp
		}
		wg.Done()
	}()
	// ---------------------------
	// Update some
	go func() {
		for _, rp := range randPoints(25, 25) {
			in <- rp
		}
		wg.Done()
	}()
	// ---------------------------
	// Delete some
	go func() {
		for i := 0; i < 25; i++ {
			in <- IndexVectorChange{Id: uint64(i + 2), Vector: nil}
		}
		wg.Done()
	}()
	// ---------------------------
	wg.Wait()
	close(in)
	require.NoError(t, <-errC)
	checkConnectivity(t, inv.nodeStore, 75)
}

func Test_EdgeScan(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	inv, err := NewIndexVamana("test", vamanaParams, bucket)
	require.NoError(t, err)
	/* Example edge scan graph:
	 * 2,3,6
	 * 3,2,4
	 * 4,3,5
	 * 5,4
	 * 6,2
	 */
	// Some points on disk, some in cache
	inv.nodeStore.Put(2, &graphNode{Id: 2, edges: []uint64{3, 6}})
	inv.nodeStore.Put(3, &graphNode{Id: 3, edges: []uint64{2, 4}})
	inv.nodeStore.Put(4, &graphNode{Id: 4, edges: []uint64{3, 5}})
	inv.nodeStore.Flush()
	inv, err = NewIndexVamana("test", vamanaParams, bucket)
	require.NoError(t, err)
	inv.nodeStore.Put(5, &graphNode{Id: 5, edges: []uint64{4}})
	inv.nodeStore.Put(6, &graphNode{Id: 6, edges: []uint64{2}})
	// ---------------------------
	// We are deleting 3 and 4
	deleteSet := map[uint64]struct{}{
		3: {},
		4: {},
	}
	// ---------------------------
	toPrune, toSave, err := inv.EdgeScan(deleteSet)
	slices.Sort(toPrune)
	slices.Sort(toSave)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 5}, toPrune)
	require.Equal(t, []uint64{5}, toSave)
}

func Test_Flush(t *testing.T) {
	bucket := diskstore.NewMemBucket(false)
	inv, err := NewIndexVamana("test", vamanaParams, bucket)
	require.NoError(t, err)
	ctx := context.Background()
	in := utils.ProduceWithContext(ctx, randPoints(42, 0))
	errC := inv.InsertUpdateDelete(ctx, in)
	require.NoError(t, <-errC)
	// ---------------------------
	// We are looking for 42 vectors, edges and ids in the bucket
	vecCount := 0
	edgeCount := 0
	maxId := 0
	err = bucket.ForEach(func(key []byte, value []byte) error {
		if bytes.Equal(key, []byte(MAXNODEIDKEY)) {
			maxId = int(conversion.BytesToUint64(value))
			return nil
		}
		suffix := key[len(key)-1]
		switch suffix {
		case 'v':
			vecCount++
		case 'e':
			edgeCount++
		default:
			require.FailNowf(t, "unexpected key", "key=%s", key)
		}
		return nil
	})
	require.NoError(t, err)
	// +1 for the start node
	require.Equal(t, 43, vecCount)
	require.Equal(t, 43, edgeCount)
	require.Equal(t, 43, maxId)
}

func Test_EmptySearch(t *testing.T) {
	inv, err := NewIndexVamana("test", vamanaParams, diskstore.NewMemBucket(false))
	require.NoError(t, err)
	checkConnectivity(t, inv.nodeStore, 0)
	// ---------------------------
	// Search
	res, err := inv.Search(context.Background(), []float32{0.5, 0.5}, 10)
	require.NoError(t, err)
	require.Len(t, res, 0)
}

func Test_Search(t *testing.T) {
	inv, err := NewIndexVamana("test", vamanaParams, diskstore.NewMemBucket(false))
	require.NoError(t, err)
	// Pre-insert
	rps := randPoints(200, 0)
	ctx := context.Background()
	in := utils.ProduceWithContext(ctx, rps)
	errC := inv.InsertUpdateDelete(ctx, in)
	require.NoError(t, <-errC)
	// ---------------------------
	// Search
	for _, rp := range rps {
		res, err := inv.Search(context.Background(), rp.Vector, 10)
		require.NoError(t, err)
		require.Len(t, res, 10)
		require.Equal(t, rp.Id, res[0].NodeId)
	}
}
