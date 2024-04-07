//go:generate go build -buildmode=c-shared -o shardpy.so shardpy.go
package main

import (
	"C"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
	"github.com/semafind/semadb/shard/cache"
	"github.com/vmihailenco/msgpack/v5"
)

/* This module provides a C interface to the shard package. It is used by the
 * Python wrapper to interact with the shard directly without any overhead.
 * Common use case is profiling the shard more easily with real data from
 * Python. */

var globalShard *shard.Shard
var globalVectorSize int

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

var collection = models.Collection{
	Id:       "benchmark",
	UserId:   "benchmark",
	Replicas: 1,
	IndexSchema: models.IndexSchema{
		"vector": models.IndexSchemaValue{
			Type: models.IndexTypeVectorVamana,
			VectorVamana: &models.IndexVectorVamanaParameters{
				VectorSize:     0,  // Set in initShard
				DistanceMetric: "", // Set in initShard
				SearchSize:     75,
				DegreeBound:    64,
				Alpha:          1.2,
				Quantizer: &models.Quantizer{
					Type: models.QuantizerNone,
					Binary: &models.BinaryQuantizerParamaters{
						TriggerThreshold: 50000,
					},
					Product: &models.ProductQuantizerParameters{
						NumCentroids:     256,
						NumSubVectors:    0, // Set in initShard
						TriggerThreshold: 10000,
					},
				},
			},
			VectorFlat: &models.IndexVectorFlatParameters{
				VectorSize:     0,  // Set in initShard
				DistanceMetric: "", // Set in initShard
			},
		},
	},
}

//export initShard
func initShard(cfgStr *C.char, metric *C.char, vectorSize int) {
	globalVectorSize = vectorSize
	// ---------------------------
	collection.IndexSchema["vector"].VectorVamana.VectorSize = uint(vectorSize)
	collection.IndexSchema["vector"].VectorVamana.DistanceMetric = C.GoString(metric)
	collection.IndexSchema["vector"].VectorFlat.VectorSize = uint(vectorSize)
	collection.IndexSchema["vector"].VectorFlat.DistanceMetric = C.GoString(metric)
	config := C.GoString(cfgStr)
	switch config {
	case "none":
	case "bq":
		collection.IndexSchema["vector"].VectorVamana.Quantizer.Type = models.QuantizerBinary
	case "pq":
		collection.IndexSchema["vector"].VectorVamana.Quantizer.Type = models.QuantizerProduct
		subcount := vectorSize / 8
		for vectorSize%subcount != 0 {
			subcount++
		}
		collection.IndexSchema["vector"].VectorVamana.Quantizer.Product.NumSubVectors = subcount
		fmt.Println("Sub Vector Count", subcount)
	default:
		log.Fatal("Invalid config", config)
	}
	// ---------------------------
	// Leaving path blank means use memory, cache manager -1 means cache is
	// never evicted. So this creates a full in memory shard.
	s, err := shard.NewShard("", collection, cache.NewManager(-1))
	if err != nil {
		log.Fatal(err)
	}
	globalShard = s
}

//export fit
func fit(X []float32) {
	// ---------------------------
	// profileFile, _ := os.Create("../semadb/dump/cpu.prof")
	// defer profileFile.Close()
	// pprof.StartCPUProfile(profileFile)
	// defer pprof.StopCPUProfile()
	// ---------------------------
	// X is flattened, so we iterate
	if len(X)%globalVectorSize != 0 {
		log.Fatal("Invalid vector size", len(X), globalVectorSize)
	}
	numVecs := len(X) / globalVectorSize
	batch_size := 10000
	for i := 0; i < numVecs; i += batch_size {
		end := min(i+batch_size, numVecs)
		fmt.Println("Fitting", i, end)
		points := make([]models.Point, end-i)
		for j := i; j < end; j++ {
			vector := make([]float32, globalVectorSize)
			copy(vector, X[j*globalVectorSize:(j+1)*globalVectorSize])
			pointData := models.PointAsMap{
				"vector": vector,
				"xid":    j,
			}
			pointDataBytes, err := msgpack.Marshal(pointData)
			if err != nil {
				log.Fatal(err)
			}
			points[j-i] = models.Point{
				Id:   uuid.New(),
				Data: pointDataBytes,
			}
		}
		if err := globalShard.InsertPoints(points); err != nil {
			log.Fatal(err)
		}
	}
}

//export startProfile
func startProfile() {
	relativePath := "../semadb/dump/cpu.prof"
	// relativePath := "../../dump/cpu.prof"
	fpath, err := filepath.Abs(relativePath)
	if err != nil {
		log.Fatal(err)
	}
	profileFile, _ := os.Create(fpath)
	fmt.Println("Starting profile - ", fpath)
	pprof.StartCPUProfile(profileFile)
}

//export stopProfile
func stopProfile() {
	fmt.Println("Stopping profile")
	pprof.StopCPUProfile()
}

/* The query accepts an out array instead of returning one because the return
 * types get ugly between Go, C and Python. It is easier to just pass the array
 * in and fill it in Go. */

//export query
func query(x []float32, k int, out []uint32) {
	sr := models.SearchRequest{
		Query: models.Query{
			Property: "vector",
			VectorVamana: &models.SearchVectorVamanaOptions{
				Vector: x,
				Limit:  k,
			},
			VectorFlat: &models.SearchVectorFlatOptions{
				Vector: x,
				Limit:  k,
			},
		},
		Select: []string{"xid"},
	}
	res, err := globalShard.SearchPoints(sr)
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	if len(out) < len(res) {
		log.Fatal("Output array too small")
	}
	var m models.PointAsMap
	for i, r := range res {
		err := msgpack.Unmarshal(r.Data, &m)
		if err != nil {
			log.Fatal(err)
		}
		out[i] = convertToUint32(m["xid"])
	}
}

func convertToUint32(in any) uint32 {
	switch v := in.(type) {
	case int:
		return uint32(v)
	case int8:
		return uint32(v)
	case int16:
		return uint32(v)
	case int32:
		return uint32(v)
	case int64:
		return uint32(v)
	case uint:
		return uint32(v)
	case uint8:
		return uint32(v)
	case uint16:
		return uint32(v)
	case uint32:
		return v
	}
	log.Fatal("Invalid type", in)
	return 0
}

// The main function is required for the build process
func main() {}
