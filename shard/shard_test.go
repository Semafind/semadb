package shard

import (
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/semafind/semadb/internal/loadhdf5"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"
)

var sampleCol models.Collection = models.Collection{
	UserId:     "test",
	Id:         "test",
	VectorSize: 2,
	DistMetric: "euclidean",
	Replicas:   1,
	Algorithm:  "vamana",
	Parameters: models.DefaultVamanaParameters(),
}

func getPointCount(shard *Shard) (count int64) {
	shard.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		b.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'v' {
				count++
			}
			return nil
		})
		return nil
	})
	count-- // Subtract one for the start point
	return
}

func getPointEdgeCount(shard *Shard, pointId uuid.UUID) (count int) {
	shard.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		point, err := getPoint(b, pointId)
		if err != nil {
			count = -1
			return err
		}
		count = len(point.Edges)
		return nil
	})
	return
}

func checkPointCount(t *testing.T, shard *Shard, expected int64) {
	assert.Equal(t, expected, getPointCount(shard))
	si, err := shard.Info()
	assert.NoError(t, err)
	assert.Equal(t, expected, si.PointCount)
}

func checkNoReferences(t *testing.T, shard *Shard, pointIds ...uuid.UUID) {
	pointIdSet := make(map[uuid.UUID]struct{})
	for _, id := range pointIds {
		pointIdSet[id] = struct{}{}
	}
	shard.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(POINTSKEY)
		b.ForEach(func(k, v []byte) error {
			if k[len(k)-1] == 'e' {
				foundId := uuid.UUID(k[:16])
				assert.NotContains(t, pointIdSet, foundId)
				edges := bytesToEdgeList(v)
				for _, edge := range edges {
					assert.NotContains(t, pointIdSet, edge)
				}
			}
			return nil
		})
		return nil
	})
}

/*func dumpEdgesToCSV(t *testing.T, shard *Shard, fpath string) {
	assert.Equal(t, ".csv", filepath.Ext(fpath))
	// ---------------------------
	// Dump to csv file
	f, err := os.Create(fpath)
	assert.NoError(t, err)
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
		randIndex := rand.Intn(16)
		// We're using a slice of the id as random metadata
		points[i] = models.Point{
			Id:       id,
			Vector:   randVector,
			Metadata: id[:randIndex],
		}
	}
	return points
}

func tempShard(t *testing.T) *Shard {
	dbpath := filepath.Join(t.TempDir(), "sharddb.bbolt")
	shard, err := NewShard(dbpath, sampleCol)
	assert.NoError(t, err)
	return shard
}

func TestShard_CreatePoint(t *testing.T) {
	shard := tempShard(t)
	// Check that the shard is empty
	checkPointCount(t, shard, 0)
	points := randPoints(2)
	err := shard.InsertPoints(points)
	assert.NoError(t, err)
	// Check that the shard has two points
	checkPointCount(t, shard, 2)
	assert.NoError(t, shard.Close())
}

func TestShard_CreateMorePoints(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(4242)
	err := shard.InsertPoints(points)
	assert.NoError(t, err)
	checkPointCount(t, shard, 4242)
	assert.NoError(t, shard.Close())
}

func TestShard_Persistence(t *testing.T) {
	shardDir := t.TempDir()
	dbfile := filepath.Join(shardDir, "sharddb.bbolt")
	shard, _ := NewShard(dbfile, sampleCol)
	points := randPoints(7)
	err := shard.InsertPoints(points)
	assert.NoError(t, err)
	assert.NoError(t, shard.Close())
	shard, err = NewShard(dbfile, sampleCol)
	assert.NoError(t, err)
	// Does the shard still have the points?
	checkPointCount(t, shard, 7)
	assert.NoError(t, shard.Close())
}

func TestShard_DuplicatePointId(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	points[0].Id = points[1].Id
	err := shard.InsertPoints(points)
	// Insert expects unique ids and should fail
	assert.Error(t, err)
	checkPointCount(t, shard, 0)
	assert.NoError(t, shard.Close())
}

func TestShard_BasicSearch(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	points[0].Metadata = []byte("test")
	shard.InsertPoints(points)
	res, err := shard.SearchPoints(points[0].Vector, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, points[0].Id, res[0].Point.Id)
	assert.Equal(t, points[0].Vector, res[0].Point.Vector)
	assert.Equal(t, points[0].Metadata, res[0].Point.Metadata)
	assert.EqualValues(t, 0, res[0].Distance)
	assert.NoError(t, shard.Close())
}

func TestShard_SearchMaxLimit(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	shard.InsertPoints(points)
	res, err := shard.SearchPoints(points[0].Vector, 7)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(res))
	assert.NoError(t, shard.Close())
}

func TestShard_UpdatePoint(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	err := shard.InsertPoints(points[:1])
	assert.NoError(t, err)
	updateRes, err := shard.UpdatePoints(points)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(updateRes))
	assert.Contains(t, updateRes, points[0].Id)
	assert.NotContains(t, updateRes, points[1].Id)
	assert.NoError(t, shard.Close())
}

func TestShard_DeletePoint(t *testing.T) {
	shard := tempShard(t)
	points := randPoints(2)
	shard.InsertPoints(points)
	deleteSet := make(map[uuid.UUID]struct{})
	deleteSet[points[0].Id] = struct{}{}
	// delete one point
	delIds, err := shard.DeletePoints(deleteSet)
	assert.NoError(t, err)
	assert.Len(t, delIds, 1)
	assert.Equal(t, points[0].Id, delIds[0])
	checkPointCount(t, shard, 1)
	checkNoReferences(t, shard, points[0].Id)
	// Try deleting the same point again
	delIds, err = shard.DeletePoints(deleteSet)
	assert.NoError(t, err)
	assert.Len(t, delIds, 0)
	checkPointCount(t, shard, 1)
	checkNoReferences(t, shard, points[0].Id)
	// Delete other point too
	deleteSet[points[1].Id] = struct{}{}
	delIds, err = shard.DeletePoints(deleteSet)
	assert.Len(t, delIds, 1)
	assert.Equal(t, points[1].Id, delIds[0])
	assert.NoError(t, err)
	checkPointCount(t, shard, 0)
	checkNoReferences(t, shard, points[0].Id, points[1].Id)
	assert.NoError(t, shard.Close())
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
	assert.NoError(t, err)
	assert.Len(t, delIds, 2)
	checkPointCount(t, shard, 0)
	assert.Equal(t, 0, getPointEdgeCount(shard, shard.startId))
	checkNoReferences(t, shard, delIds...)
	// Try searching for the deleted point
	res, err := shard.SearchPoints(points[0].Vector, 1)
	assert.NoError(t, err)
	assert.Len(t, res, 0)
	// Try inserting the deleted points
	err = shard.InsertPoints(points)
	assert.NoError(t, err)
	checkPointCount(t, shard, 2)
	assert.NoError(t, shard.Close())
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
	assert.NoError(t, err)
	assert.Len(t, delIds, delSize)
	checkPointCount(t, shard, int64(initSize-delSize))
	checkNoReferences(t, shard, delIds...)
	// Try inserting the deleted points
	err = shard.InsertPoints(points[:delSize])
	assert.NoError(t, err)
	checkPointCount(t, shard, int64(initSize))
	// Try searching for the deleted point
	sp := points[0]
	res, err := shard.SearchPoints(sp.Vector, 1)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, sp.Id, res[0].Point.Id)
	assert.NoError(t, shard.Close())
}

func TestShard_LargeInsertUpdateSearch(t *testing.T) {
	shard := tempShard(t)
	initSize := 10000
	points := randPoints(initSize)
	shard.InsertPoints(points)
	// Update half the points
	updateSize := initSize / 2
	updatePoints := randPoints(updateSize)
	for i := 0; i < updateSize; i++ {
		updatePoints[i].Id = points[i].Id
	}
	updateRes, err := shard.UpdatePoints(updatePoints)
	assert.NoError(t, err)
	assert.Len(t, updateRes, updateSize)
	checkPointCount(t, shard, int64(initSize))
	// Try searching for the updated point
	res, err := shard.SearchPoints(updatePoints[0].Vector, 1)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, points[0].Id, res[0].Point.Id)
	assert.NoError(t, shard.Close())
}

func TestShard_InsertSinglePoint(t *testing.T) {
	// ---------------------------
	/* This test is disabled by default because it takes a long time to run. We
	 * are not setting this as a benchmark because we are interested in the total
	 * time taken to insert all the points, not the time taken to insert a single
	 * point. */
	// Run using go test -v -timeout 2m -run ^TestShard_InsertSinglePoint$ github.com/semafind/semadb/shard
	t.Skip("Skipping benchmark test")
	// ---------------------------
	numPoints := 10000
	// ---------------------------
	// Find all dataset files in data folder ending with hdf5
	datasetFiles, err := filepath.Glob("../data/*.hdf5")
	assert.NoError(t, err)
	t.Log("Found", len(datasetFiles), "files:", datasetFiles)
	// ---------------------------
	// Disable zerolog
	zerolog.SetGlobalLevel(zerolog.Disabled)
	// ---------------------------
	profileFile, _ := os.Create("../dump/cpu.prof")
	defer profileFile.Close()
	pprof.StartCPUProfile(profileFile)
	defer pprof.StopCPUProfile()
	// ---------------------------
	for _, datasetFile := range datasetFiles {
		t.Run(filepath.Base(datasetFile), func(t *testing.T) {
			vecCol, err := loadhdf5.LoadHDF5(datasetFile)
			assert.NoError(t, err)
			col := models.Collection{
				UserId:     "test",
				Id:         strings.Split(filepath.Base(datasetFile), ".")[0],
				VectorSize: uint(len(vecCol.Vectors[0])),
				DistMetric: vecCol.DistMetric,
				Replicas:   1,
				Algorithm:  "vamana",
				Parameters: models.DefaultVamanaParameters(),
			}
			t.Log("Loaded", len(vecCol.Vectors), "vectors of size", col.VectorSize, "from", datasetFile)
			// ---------------------------
			dbpath := filepath.Join(t.TempDir(), "sharddb.bbolt")
			shard, err := NewShard(dbpath, col)
			assert.NoError(t, err)
			// ---------------------------
			maxPoints := min(numPoints, len(vecCol.Vectors))
			err = shard.db.Update(func(tx *bbolt.Tx) error {
				buc := tx.Bucket(POINTSKEY)
				pc := NewPointCache(buc)
				startTime := time.Now()
				for i := 0; i < maxPoints; i++ {
					// Create a random point
					point := models.Point{
						Id:       uuid.New(),
						Vector:   vecCol.Vectors[i],
						Metadata: []byte("test"),
					}
					shard.insertSinglePoint(pc, shard.startId, ShardPoint{Point: point})
				}
				t.Log("Insert took", time.Since(startTime))
				return pc.Flush()
			})
			assert.NoError(t, err)
			// ---------------------------
			assert.NoError(t, shard.Close())
		})
	}
	// ---------------------------
}
