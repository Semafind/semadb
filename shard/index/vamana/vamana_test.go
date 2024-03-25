package vamana_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index/vamana"
	"github.com/stretchr/testify/require"
)

var vamanaParams = models.IndexVectorVamanaParameters{
	VectorSize:     2,
	DistanceMetric: "euclidean",
	SearchSize:     75,
	DegreeBound:    64,
	Alpha:          1.2,
}

func checkConnectivity(t *testing.T, pc cache.SharedPointCache, expected int) {
	visited := make(map[uint64]struct{})
	queue := make([]uint64, 0)
	queue = append(queue, vamana.STARTID)
	for len(queue) > 0 {
		pointId := queue[0]
		queue = queue[1:]
		if _, ok := visited[pointId]; ok {
			continue
		}
		visited[pointId] = struct{}{}
		point, err := pc.GetPoint(pointId)
		require.NoError(t, err)
		err = pc.WithPointNeighbours(point, true, func(neighbours []*cache.CachePoint) error {
			for _, neighbour := range neighbours {
				queue = append(queue, neighbour.NodeId)
			}
			return nil
		})
		require.NoError(t, err)
	}
	// -1 for the start node
	require.Equal(t, expected, len(visited)-1)
}

func randPoints(size int, offset int) []cache.GraphNode {
	points := make([]cache.GraphNode, size)
	for i := 0; i < size; i++ {
		randVector := make([]float32, 2)
		randVector[0] = rand.Float32()
		randVector[1] = rand.Float32()
		points[i] = cache.GraphNode{
			// 0 is not allowed, 1 is start node
			NodeId: uint64(i + offset + 2),
			Vector: randVector,
		}
	}
	return points
}

func Test_Insert(t *testing.T) {
	for _, size := range []int{1, 100, 4242} {
		t.Run(fmt.Sprintf("Size=%d", size), func(t *testing.T) {
			pc := cache.NewMemPointCache()
			inv, err := vamana.NewIndexVamana("test", pc, vamanaParams, uint64(size))
			require.NoError(t, err)
			// Pre-insert
			in := make(chan cache.GraphNode)
			errC := inv.InsertUpdateDelete(context.Background(), in)
			for _, rp := range randPoints(size, 0) {
				in <- rp
			}
			close(in)
			require.NoError(t, <-errC)
			checkConnectivity(t, pc, size)
		})
	}
}

func Test_ConcurrentCUD(t *testing.T) {
	pc := cache.NewMemPointCache()
	inv, err := vamana.NewIndexVamana("test", pc, vamanaParams, 200)
	require.NoError(t, err)
	// Pre-insert
	in := make(chan cache.GraphNode)
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
			in <- cache.GraphNode{NodeId: uint64(i + 2), Vector: nil}
		}
		wg.Done()
	}()
	// ---------------------------
	wg.Wait()
	close(in)
	require.NoError(t, <-errC)
	checkConnectivity(t, pc, 75)
}

func Test_EmptySearch(t *testing.T) {
	pc := cache.NewMemPointCache()
	inv, err := vamana.NewIndexVamana("test", pc, vamanaParams, 200)
	require.NoError(t, err)
	checkConnectivity(t, pc, 0)
	// ---------------------------
	// Search
	res, err := inv.Search(context.Background(), []float32{0.5, 0.5}, 10)
	require.NoError(t, err)
	require.Len(t, res, 0)
}

func Test_Search(t *testing.T) {
	pc := cache.NewMemPointCache()
	inv, err := vamana.NewIndexVamana("test", pc, vamanaParams, 200)
	require.NoError(t, err)
	// Pre-insert
	in := make(chan cache.GraphNode)
	errC := inv.InsertUpdateDelete(context.Background(), in)
	rps := randPoints(200, 0)
	for _, rp := range rps {
		in <- rp
	}
	close(in)
	require.NoError(t, <-errC)
	// ---------------------------
	// Search
	for _, rp := range rps {
		res, err := inv.Search(context.Background(), rp.Vector, 10)
		require.NoError(t, err)
		require.Len(t, res, 10)
		require.Equal(t, rp.NodeId, res[0].NodeId)
	}
}
