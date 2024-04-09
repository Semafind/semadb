package shard

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/semafind/semadb/conversion"
	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard/cache"
	"github.com/semafind/semadb/shard/index/vamana"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

var sampleIndexSchema = models.IndexSchema{
	"vector": models.IndexSchemaValue{
		Type: models.IndexTypeVectorVamana,
		VectorVamana: &models.IndexVectorVamanaParameters{
			VectorSize:     2,
			DistanceMetric: "euclidean",
			SearchSize:     75,
			DegreeBound:    64,
			Alpha:          1.2,
		},
	},
	"flat": models.IndexSchemaValue{
		Type: models.IndexTypeVectorFlat,
		VectorFlat: &models.IndexVectorFlatParameters{
			VectorSize:     2,
			DistanceMetric: "euclidean",
		},
	},
	"description": models.IndexSchemaValue{
		Type: models.IndexTypeText,
		Text: &models.IndexTextParameters{
			Analyser: "standard",
		},
	},
	"category": models.IndexSchemaValue{
		Type: models.IndexTypeString,
		String: &models.IndexStringParameters{
			CaseSensitive: false,
		},
	},
	"labels": models.IndexSchemaValue{
		Type: models.IndexTypeStringArray,
		StringArray: &models.IndexStringArrayParameters{
			IndexStringParameters: models.IndexStringParameters{
				CaseSensitive: false,
			},
		},
	},
	"size": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
	"price": models.IndexSchemaValue{
		Type: models.IndexTypeFloat,
	},
	"nonExistent": models.IndexSchemaValue{
		Type: models.IndexTypeInteger,
	},
}

var sampleCol models.Collection = models.Collection{
	UserId:      "test",
	Id:          "test",
	Replicas:    1,
	IndexSchema: sampleIndexSchema,
	UserPlan: models.UserPlan{
		Name:                    "test",
		MaxCollections:          1,
		MaxCollectionPointCount: 100000,
		MaxMetadataSize:         100000,
		ShardBackupFrequency:    0,
		ShardBackupCount:        0,
	},
}

const GRAPHINDEXBUCKETKEY = "index/vectorVamana/vector"

func getVectorCount(shard *Shard) (count int) {
	shard.db.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(GRAPHINDEXBUCKETKEY)
		if err != nil {
			count = 0
			return err
		}
		b.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'v' {
				count++
			}
			return nil
		})
		return nil
	})
	if count > 0 {
		count-- // Subtract one for the start point
	}
	return
}

func getPointEdgeCount(shard *Shard, nodeId uint64) (count int) {
	shard.db.Read(func(bm diskstore.BucketManager) error {
		b, _ := bm.Get(GRAPHINDEXBUCKETKEY)
		n := b.Get(conversion.NodeKey(nodeId, 'e'))
		count = len(n) / 8
		return nil
	})
	return
}

func checkConnectivity(t *testing.T, shard *Shard, expectedCount int) {
	// Perform a BFS from the start point
	visited := make(map[uint64]struct{})
	queue := make([]uint64, 0)
	queue = append(queue, vamana.STARTID)
	err := shard.db.Read(func(bm diskstore.BucketManager) error {
		graphBucket, err := bm.Get(GRAPHINDEXBUCKETKEY)
		require.NoError(t, err)
		// ---------------------------
		for len(queue) > 0 {
			nodeId := queue[0]
			queue = queue[1:]
			if _, ok := visited[nodeId]; ok {
				continue
			}
			visited[nodeId] = struct{}{}
			edgeBytes := graphBucket.Get(conversion.NodeKey(nodeId, 'e'))
			require.NotNil(t, edgeBytes)
			edges := conversion.BytesToEdgeList(edgeBytes)
			queue = append(queue, edges...)
		}
		return nil
	})
	require.NoError(t, err)
	// We subtract one because the start point is not in the database but is an
	// entry point to the graph.
	require.Equal(t, expectedCount, len(visited)-1)
}

func checkNodeIdPointIdMapping(t *testing.T, shard *Shard, expectedCount int) {
	nodeCount := 0
	pointCount := 0
	shard.db.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(POINTSBUCKETKEY)
		require.NoError(t, err)
		b.ForEach(func(k, v []byte) error {
			if k[0] == 'n' && k[len(k)-1] == 'i' {
				pointId := uuid.UUID(v)
				nodeId := conversion.BytesToUint64(k[1 : len(k)-1])
				reverseId := b.Get(PointKey(pointId, 'i'))
				require.Equal(t, nodeId, conversion.BytesToUint64(reverseId))
				nodeCount++
			}
			if k[0] == 'p' && k[len(k)-1] == 'i' {
				pointId := uuid.UUID(k[1 : len(k)-1])
				nodeId := conversion.BytesToUint64(v)
				reverseId := b.Get(conversion.NodeKey(nodeId, 'i'))
				require.Equal(t, pointId, uuid.UUID(reverseId))
				pointCount++
			}
			return nil
		})
		return nil
	})
	// We subtract one because the start point is not in the database but is an
	// entry point to the graph.
	require.Equal(t, expectedCount, nodeCount)
	require.Equal(t, expectedCount, pointCount)
}

func checkPointCount(t *testing.T, shard *Shard, expected int) {
	require.Equal(t, expected, getVectorCount(shard))
	checkNodeIdPointIdMapping(t, shard, expected)
	si, err := shard.Info()
	require.NoError(t, err)
	require.EqualValues(t, expected, si.PointCount)
	checkConnectivity(t, shard, int(expected))
}

func checkNoReferences(t *testing.T, shard *Shard, pointIds ...uuid.UUID) {
	shard.db.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(POINTSBUCKETKEY)
		require.NoError(t, err)
		// Check that the point ids are not in the database
		for _, id := range pointIds {
			nodeIdBytes := b.Get(PointKey(id, 'i'))
			require.Nil(t, nodeIdBytes)
		}
		graphBucket, err := bm.Get(GRAPHINDEXBUCKETKEY)
		require.NoError(t, err)
		// Check all remaining points have valid edges
		graphBucket.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'e' {
				nodeId, _ := conversion.NodeIdFromKey(k, 'e')
				edges := conversion.BytesToEdgeList(v)
				// Cannot have self edges
				require.NotContains(t, edges, nodeId)
				for _, edge := range edges {
					nodeVal := graphBucket.Get(conversion.NodeKey(edge, 'v'))
					require.NotNil(t, nodeVal)
				}
			}
			return nil
		})
		return nil
	})
}

func checkMaxNodeId(t *testing.T, shard *Shard, expected int) {
	var maxId uint64
	shard.db.Read(func(bm diskstore.BucketManager) error {
		b, err := bm.Get(POINTSBUCKETKEY)
		require.NoError(t, err)
		b.ForEach(func(k, v []byte) error {
			nodeId, ok := conversion.NodeIdFromKey(k, 'i')
			if ok && nodeId > maxId {
				maxId = nodeId
			}
			return nil
		})
		return nil
	})
	// We add one because the start point nodeId starts from 1. For example, if
	// we are expecting the maxId to be 3 (points), then the maximum Id will be
	// 4.
	require.LessOrEqual(t, maxId, uint64(expected+1))
}

/*func dumpEdgesToCSV(t *testing.T, shard *Shard, fpath string) {
	require.Equal(t, ".csv", filepath.Ext(fpath))
	// ---------------------------
	// Dump to csv file
	f, err := os.Create(fpath)
	require.NoError(t, err)
	defer f.Close()
	// ---------------------------
	shard.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		b.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'e' {
				pointId := uuid.UUID(k[:16])
				edges := bytesToEdgeList(v)
				// pointId, edge0, edge1, ...
				f.WriteString(pointId.String())
				for _, edge := range edges {
					f.WriteString(",")
					f.WriteString(edge.String())
				}
				f.WriteString("\n")
			}
			return nil
		})
		return nil
	})
}*/

func randPoints(size int) []models.Point {
	points := make([]models.Point, size)
	for i := 0; i < size; i++ {
		randVector := make([]float32, 2)
		randVector[0] = rand.Float32()
		randVector[1] = rand.Float32()
		id := uuid.New()
		// We're using a slice of the id as random metadata
		fi := float32(i)
		pointData := models.PointAsMap{
			"vector":      randVector,
			"flat":        []float32{fi, fi + 1},
			"description": fmt.Sprintf("This is a description %d", i),
			"category":    fmt.Sprintf("category %d", i),
			"labels":      []string{fmt.Sprintf("label1 %d", i), fmt.Sprintf("label2 %d", i+1)},
			"size":        i,
			"price":       fi + 0.5,
		}
		if rand.Float32() < 0.5 {
			pointData["extra"] = fmt.Sprintf("extra %d", i%5)
		}
		if sampleIndexSchema.CheckCompatibleMap(pointData) != nil {
			panic("Incompatible map")
		}
		pointDataBytes, err := msgpack.Marshal(pointData)
		if err != nil {
			panic(err)
		}
		points[i] = models.Point{
			Id:   id,
			Data: pointDataBytes,
		}
	}
	return points
}

func getVector(p models.Point) []float32 {
	var data models.PointAsMap
	err := msgpack.Unmarshal(p.Data, &data)
	if err != nil {
		panic(err)
	}
	vectorAny := data["vector"].([]any)
	vector := make([]float32, len(vectorAny))
	for i, v := range vectorAny {
		vector[i] = v.(float32)
	}
	return vector
}

func tempShard(t *testing.T) *Shard {
	dbpath := filepath.Join(t.TempDir(), "sharddb.bbolt")
	shard, err := NewShard(dbpath, sampleCol, cache.NewManager(-1))
	require.NoError(t, err)
	return shard
}

func searchRequest(p models.Point, limit int) models.SearchRequest {
	return models.SearchRequest{
		Query: models.Query{
			Property: "vector",
			VectorVamana: &models.SearchVectorVamanaOptions{
				Vector:     getVector(p),
				SearchSize: 75,
				Limit:      limit,
				Operator:   "near",
			},
		},
		Limit: limit,
	}
}

func TestShard_CreatePoint(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(42)
	err := shard.InsertPoints(points)
	require.NoError(t, err)
	// Check that the shard has two points
	checkPointCount(t, shard, 42)
	require.NoError(t, shard.Close())
}

func TestShard_CreateMorePoints(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(4242)
	err := shard.InsertPoints(points)
	require.NoError(t, err)
	checkPointCount(t, shard, 4242)
	require.NoError(t, shard.Close())
}

func TestShard_Persistence(t *testing.T) {
	shardDir := t.TempDir()
	dbfile := filepath.Join(shardDir, "sharddb.bbolt")
	shard, _ := NewShard(dbfile, sampleCol, cache.NewManager(-1))
	points := randPoints(7)
	err := shard.InsertPoints(points)
	require.NoError(t, err)
	require.NoError(t, shard.Close())
	shard, err = NewShard(dbfile, sampleCol, cache.NewManager(-1))
	require.NoError(t, err)
	// Does the shard still have the points?
	checkPointCount(t, shard, 7)
	require.NoError(t, shard.Close())
}

func TestShard_DuplicatePointId(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	points[0].Id = points[1].Id
	err := shard.InsertPoints(points)
	// Insert expects unique ids and should fail
	require.Error(t, err)
	require.NoError(t, shard.Close())
}

func TestShard_BasicSearch(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	shard.InsertPoints(points)
	res, err := shard.SearchPoints(searchRequest(points[0], 1))
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, points[0].Id, res[0].Point.Id)
	require.Equal(t, getVector(points[0]), getVector(res[0].Point))
	require.Equal(t, points[0].Data, res[0].Point.Data)
	require.EqualValues(t, 0, *res[0].Distance)
	require.NoError(t, shard.Close())
}

func TestShard_CacheReuse(t *testing.T) {
	cm := cache.NewManager(-1)
	shard, _ := NewShard("", sampleCol, cm)
	points := randPoints(7)
	err := shard.InsertPoints(points)
	require.NoError(t, err)
	// Purge the disk storage layer
	err = shard.db.Write(func(bm diskstore.BucketManager) error {
		return bm.Delete(GRAPHINDEXBUCKETKEY)
	})
	require.NoError(t, err)
	// The shared cache should allow us to search
	res, err := shard.SearchPoints(searchRequest(points[0], 1))
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, points[0].Id, res[0].Point.Id)
	require.Equal(t, getVector(points[0]), getVector(res[0].Point))
	require.Equal(t, points[0].Data, res[0].Point.Data)
	require.EqualValues(t, 0, *res[0].Distance)
	require.NoError(t, shard.Close())
}

func TestShard_BucketSearch(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	require.NoError(t, shard.InsertPoints(points))
	// Clear the cache
	shard.cacheManager.Release(shard.dbFile + "/index/vectorVamana/vector")
	// Search from the bucket directly
	res, err := shard.SearchPoints(searchRequest(points[0], 1))
	require.NoError(t, err)
	require.Equal(t, 1, len(res))
	require.Equal(t, points[0].Id, res[0].Point.Id)
	require.Equal(t, getVector(points[0]), getVector(res[0].Point))
	require.Equal(t, points[0].Data, res[0].Point.Data)
	require.EqualValues(t, 0, *res[0].Distance)
	require.NoError(t, shard.Close())
}

func TestShard_SearchMaxLimit(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	shard.InsertPoints(points)
	res, err := shard.SearchPoints(searchRequest(points[0], 7))
	require.NoError(t, err)
	require.Equal(t, 2, len(res))
	require.NoError(t, shard.Close())
}

func TestShard_UpdatePoint(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	err := shard.InsertPoints(points[:1])
	require.NoError(t, err)
	updateRes, err := shard.UpdatePoints(points)
	require.NoError(t, err)
	require.Equal(t, 1, len(updateRes))
	require.Contains(t, updateRes, points[0].Id)
	require.NotContains(t, updateRes, points[1].Id)
	require.NoError(t, shard.Close())
}

func TestShard_DeletePoint(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	shard.InsertPoints(points)
	deleteSet := make(map[uuid.UUID]struct{})
	deleteSet[points[0].Id] = struct{}{}
	// delete one point
	delIds, err := shard.DeletePoints(deleteSet)
	require.NoError(t, err)
	require.Len(t, delIds, 1)
	require.Equal(t, points[0].Id, delIds[0])
	checkPointCount(t, shard, 1)
	checkMaxNodeId(t, shard, 2)
	checkNoReferences(t, shard, points[0].Id)
	// Try deleting the same point again
	delIds, err = shard.DeletePoints(deleteSet)
	require.NoError(t, err)
	require.Len(t, delIds, 0)
	checkPointCount(t, shard, 1)
	checkNoReferences(t, shard, points[0].Id)
	// Delete other point too
	deleteSet[points[1].Id] = struct{}{}
	delIds, err = shard.DeletePoints(deleteSet)
	require.Len(t, delIds, 1)
	require.Equal(t, points[1].Id, delIds[0])
	require.NoError(t, err)
	checkPointCount(t, shard, 0)
	checkNoReferences(t, shard, points[0].Id, points[1].Id)
	require.NoError(t, shard.Close())
}

func TestShard_InsertDeleteSearchInsertPoint(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	shard.InsertPoints(points)
	deleteSet := make(map[uuid.UUID]struct{})
	deleteSet[points[0].Id] = struct{}{}
	deleteSet[points[1].Id] = struct{}{}
	// delete all points
	delIds, err := shard.DeletePoints(deleteSet)
	require.NoError(t, err)
	require.Len(t, delIds, 2)
	checkPointCount(t, shard, 0)
	require.Equal(t, 0, getPointEdgeCount(shard, vamana.STARTID))
	checkNoReferences(t, shard, delIds...)
	checkMaxNodeId(t, shard, 0)
	// Try searching for the deleted point
	res, err := shard.SearchPoints(searchRequest(points[0], 1))
	require.NoError(t, err)
	require.Len(t, res, 0)
	// Try inserting the deleted points
	err = shard.InsertPoints(points)
	require.NoError(t, err)
	checkPointCount(t, shard, 2)
	checkMaxNodeId(t, shard, 2)
	require.NoError(t, shard.Close())
}

func TestShard_SearchWhileInsert(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(100)
	// Insert points
	err := shard.InsertPoints(points)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		newPoints := randPoints(100)
		err := shard.InsertPoints(newPoints)
		assert.NoError(t, err)
		wg.Done()
	}()
	// Search points
	go func() {
		for _, point := range points {
			res, err := shard.SearchPoints(searchRequest(point, 1))
			assert.NoError(t, err)
			assert.Len(t, res, 1)
			assert.Equal(t, point.Id, res[0].Point.Id)
		}
		wg.Done()
	}()
	wg.Wait()
	checkPointCount(t, shard, 200)
	require.NoError(t, shard.Close())
}

func TestShard_DeleteWhileInsert(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(3)
	// Insert points
	err := shard.InsertPoints(points)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		newPoints := randPoints(3)
		err := shard.InsertPoints(newPoints)
		assert.NoError(t, err)
		wg.Done()
	}()
	go func() {
		// Delete points
		deleteSet := make(map[uuid.UUID]struct{})
		for i := 0; i < 2; i++ {
			deleteSet[points[i].Id] = struct{}{}
		}
		delIds, err := shard.DeletePoints(deleteSet)
		assert.NoError(t, err)
		assert.Len(t, delIds, 2)
		wg.Done()
	}()
	wg.Wait()
	checkPointCount(t, shard, 4)
	require.NoError(t, shard.Close())
}

func TestShard_ConcurrentCRUD(t *testing.T) {
	// We'll insert, search, update and delete at the same time
	shard := tempShard(t)
	points := randPoints(150)
	// Initial points
	require.NoError(t, shard.InsertPoints(points))
	var wg sync.WaitGroup
	wg.Add(5)
	// ---------------------------
	// Insert more points
	go func() {
		// Insert points
		newPoints := randPoints(50)
		assert.NoError(t, shard.InsertPoints(newPoints))
		wg.Done()
	}()
	go func() {
		// Insert points
		newPoints := randPoints(50)
		assert.NoError(t, shard.InsertPoints(newPoints))
		wg.Done()
	}()
	// ---------------------------
	// Search points
	go func() {
		for i := 0; i < 50; i++ {
			res, err := shard.SearchPoints(searchRequest(points[i], 1))
			assert.NoError(t, err)
			assert.Len(t, res, 1)
			assert.Equal(t, points[i].Id, res[0].Point.Id)
		}
		wg.Done()
	}()
	// ---------------------------
	// Update points
	go func() {
		updatePoints := randPoints(50)
		for i := 50; i < 100; i++ {
			updatePoints[i-50].Id = points[i].Id
		}
		updateRes, err := shard.UpdatePoints(updatePoints)
		assert.NoError(t, err)
		assert.Len(t, updateRes, 50)
		wg.Done()
	}()
	// ---------------------------
	// Delete points
	go func() {
		deleteSet := make(map[uuid.UUID]struct{})
		for i := 100; i < 150; i++ {
			deleteSet[points[i].Id] = struct{}{}
		}
		delIds, err := shard.DeletePoints(deleteSet)
		assert.NoError(t, err)
		assert.Len(t, delIds, 50)
		wg.Done()
	}()
	// ---------------------------
	wg.Wait()
	checkPointCount(t, shard, 200)
	// We can't guarantee that the max node id will be 200 or 250 because it
	// depends on the order of delete and inserts run. If both inserts run first
	// it'll go up to 250, if delete runs first then it'll be 200.
	// checkMaxNodeId(t, shard, 250)
	require.NoError(t, shard.Close())
}

func TestShard_LargeInsertDeleteInsertSearch(t *testing.T) {
	shard := tempShard(t)
	initSize := 10000
	points := randPoints(initSize)
	// Insert points
	shard.InsertPoints(points)
	// dumpEdgesToCSV(t, shard, "../dump/edgesBeforeDelete.csv")
	deleteSet := make(map[uuid.UUID]struct{})
	delSize := 500
	for i := 0; i < delSize; i++ {
		deleteSet[points[i].Id] = struct{}{}
	}
	// delete all points
	delIds, err := shard.DeletePoints(deleteSet)
	// dumpEdgesToCSV(t, shard, "../dump/edgesAfterDelete.csv")
	require.NoError(t, err)
	require.Len(t, delIds, delSize)
	checkPointCount(t, shard, initSize-delSize)
	checkNoReferences(t, shard, delIds...)
	checkMaxNodeId(t, shard, initSize)
	// Try inserting the deleted points
	err = shard.InsertPoints(points[:delSize])
	require.NoError(t, err)
	checkPointCount(t, shard, initSize)
	checkMaxNodeId(t, shard, initSize)
	// Try searching for the deleted point
	sp := points[0]
	res, err := shard.SearchPoints(searchRequest(sp, 1))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, sp.Id, res[0].Point.Id)
	require.NoError(t, shard.Close())
}

func TestShard_LargeInsertUpdateSearch(t *testing.T) {
	shard := tempShard(t)
	initSize := 10000
	points := randPoints(initSize)
	shard.InsertPoints(points)
	// Update some of the points
	updateSize := 100
	updatePoints := randPoints(updateSize)
	for i := 0; i < updateSize; i++ {
		updatePoints[i].Id = points[i].Id
	}
	updateRes, err := shard.UpdatePoints(updatePoints)
	require.NoError(t, err)
	require.Len(t, updateRes, updateSize)
	checkPointCount(t, shard, initSize)
	checkMaxNodeId(t, shard, initSize)
	// Try searching for the updated point
	res, err := shard.SearchPoints(searchRequest(updatePoints[0], 1))
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, points[0].Id, res[0].Point.Id)
	require.NoError(t, shard.Close())
}

// func TestShard_InsertSinglePoint(t *testing.T) {
// 	// ---------------------------
// 	/* This test is disabled by default because it takes a long time to run. We
// 	 * are not setting this as a benchmark because we are interested in the total
// 	 * time taken to insert all the points, not the time taken to insert a single
// 	 * point. */
// 	// Run using go test -v -timeout 2m -run ^TestShard_InsertSinglePoint$ github.com/semafind/semadb/shard
// 	t.Skip("Skipping benchmark test")
// 	// ---------------------------
// 	numPoints := 10000
// 	// ---------------------------
// 	// Find all dataset files in data folder ending with hdf5
// 	datasetFiles, err := filepath.Glob("../data/*.hdf5")
// 	require.NoError(t, err)
// 	t.Log("Found", len(datasetFiles), "files:", datasetFiles)
// 	// ---------------------------
// 	// Disable zerolog
// 	zerolog.SetGlobalLevel(zerolog.Disabled)
// 	// ---------------------------
// 	// Preload dataset files so we don't measure the time it takes to load them
// 	datasets := make(map[string]loadhdf5.VectorCollection, len(datasetFiles))
// 	for _, datasetFile := range datasetFiles {
// 		start := time.Now()
// 		vecCol, err := loadhdf5.LoadHDF5(datasetFile)
// 		require.NoError(t, err)
// 		datasets[datasetFile] = vecCol
// 		t.Log("Loaded", len(vecCol.Vectors), "vectors of size", len(vecCol.Vectors[0]), "from", datasetFile, "in", time.Since(start))
// 	}
// 	// ---------------------------
// 	profileFile, _ := os.Create("../dump/cpu.prof")
// 	defer profileFile.Close()
// 	pprof.StartCPUProfile(profileFile)
// 	defer pprof.StopCPUProfile()
// 	// ---------------------------
// 	for _, datasetFile := range datasetFiles {
// 		t.Run(filepath.Base(datasetFile), func(t *testing.T) {
// 			vecCol := datasets[datasetFile]
// 			require.NoError(t, err)
// 			col := models.Collection{
// 				UserId:     "test",
// 				Id:         strings.Split(filepath.Base(datasetFile), ".")[0],
// 				VectorSize: uint(len(vecCol.Vectors[0])),
// 				DistMetric: vecCol.DistMetric,
// 				Replicas:   1,
// 				Algorithm:  "vamana",
// 				Parameters: models.DefaultVamanaParameters(),
// 			}
// 			// ---------------------------
// 			dbpath := filepath.Join(t.TempDir(), "sharddb.bbolt")
// 			shard, err := NewShard(dbpath, col, cache.NewManager(0))
// 			require.NoError(t, err)
// 			// ---------------------------
// 			maxPoints := min(numPoints, len(vecCol.Vectors))
// 			err = shard.db.WriteMultiple([]string{POINTSBUCKETKEY, GRAPHINDEXBUCKETKEY}, func(buckets []diskstore.Bucket) error {
// 				pointsBucket := buckets[0]
// 				graphBucket := buckets[1]
// 				return shard.cacheManager.With(shard.dbFile, pointsBucket, graphBucket, func(pc cache.ReadWriteCache) error {
// 					startTime := time.Now()
// 					for i := 0; i < maxPoints; i++ {
// 						// Create a random point
// 						point := models.Point{
// 							Id:       uuid.New(),
// 							Vector:   vecCol.Vectors[i],
// 							Metadata: []byte("test"),
// 						}
// 						shard.insertSinglePoint(pc, shard.startId, cache.ShardPoint{Point: point, NodeId: uint64(i + 1)})
// 					}
// 					t.Log("Insert took", time.Since(startTime))
// 					return pc.Flush()
// 				})
// 			})
// 			require.NoError(t, err)
// 			// ---------------------------
// 			// Perform search
// 			err = shard.db.ReadMultiple([]string{POINTSBUCKETKEY, GRAPHINDEXBUCKETKEY}, func(buckets []diskstore.ReadOnlyBucket) error {
// 				pointsBucket := buckets[0]
// 				graphBucket := buckets[1]
// 				return shard.cacheManager.WithReadOnly(shard.dbFile, pointsBucket, graphBucket, func(pc cache.ReadOnlyCache) error {
// 					startTime := time.Now()
// 					for i := 0; i < maxPoints; i++ {
// 						query := vecCol.Vectors[i]
// 						k := 10
// 						searchSet, _, err := shard.greedySearch(pc, shard.startId, query, k, shard.collection.Parameters.SearchSize)
// 						if err != nil {
// 							return err
// 						}
// 						require.Greater(t, len(searchSet.items), 0)
// 					}
// 					t.Log("Search took", time.Since(startTime))
// 					return nil
// 				})
// 			})
// 			require.NoError(t, err)
// 			// ---------------------------
// 			require.NoError(t, shard.Close())
// 		})
// 	}
// 	// ---------------------------
// }
