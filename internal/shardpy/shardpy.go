//go:generate go build -buildmode=c-shared -o shardpy.so shardpy.go
package main

import (
	"C"
	"log"

	"encoding/binary"
	"fmt"
	"os"

	"runtime/pprof"

	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"

	"github.com/google/uuid"
)

/* This module provides a C interface to the shard package. It is used by the
 * Python wrapper to interact with the shard directly without any overhead.
 * Common use case is profiling the shard more easily with real data from
 * Python. */

var globalShard *shard.Shard
var globalVectorSize int

//export initShard
func initShard(dataset *C.char, metric *C.char, vectorSize int) {
	globalVectorSize = vectorSize
	dpath := fmt.Sprintf("test-%s.bbolt", C.GoString(dataset))
	// Remove the file if it exists
	if err := os.Remove(dpath); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}
	s, err := shard.NewShard(
		fmt.Sprintf("test-%s.bbolt", C.GoString(dataset)),
		models.Collection{
			Id:         C.GoString(dataset),
			VectorSize: uint(vectorSize),
			DistMetric: C.GoString(metric),
			Replicas:   1,
			Algorithm:  "vamana",
			Parameters: models.DefaultVamanaParameters(),
		},
	)
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
			var m [4]byte
			binary.LittleEndian.PutUint32(m[:], uint32(j))
			vector := make([]float32, globalVectorSize)
			copy(vector, X[j*globalVectorSize:(j+1)*globalVectorSize])
			points[j-i] = models.Point{
				Id:       uuid.New(),
				Vector:   vector,
				Metadata: m[:],
			}
		}
		if err := globalShard.InsertPoints(points); err != nil {
			log.Fatal(err)
		}
	}
}

//export startProfile
func startProfile() {
	profileFile, _ := os.Create("../semadb/dump/cpu.prof")
	fmt.Println("Starting profile")
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
	res, err := globalShard.SearchPoints(x, k)
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	if len(out) < len(res) {
		log.Fatal("Output array too small")
	}
	for i, r := range res {
		out[i] = binary.LittleEndian.Uint32(r.Point.Metadata)
	}
}

// The main function is required for the build process
func main() {}
