package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/semafind/semadb/collection"
	"gonum.org/v1/gonum/blas/blas32"
	"gonum.org/v1/hdf5"
)

// ---------------------------

func pongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong from semadb",
	})
}

// ---------------------------

var benchmarkCollection *collection.Collection

// ---------------------------

func newCollectionHandler(c *gin.Context) {
	var config collection.CollectionConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Println("config:", config)
	// ---------------------------
	if benchmarkCollection != nil {
		benchmarkCollection.Close()
		// Delete dump directory
		dbDir, ok := os.LookupEnv("DBDIR")
		if !ok {
			dbDir = "dump"
		}
		os.RemoveAll(dbDir)
	}
	// ---------------------------
	collection, err := collection.NewCollection(config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	benchmarkCollection = collection
	// ---------------------------
	c.JSON(http.StatusOK, gin.H{
		"collectionId": collection.Id,
	})
}

// ---------------------------

type AddParams struct {
	Entries []collection.Entry `json:"entries"`
}

func collectionPutHandler(c *gin.Context) {
	collectionId := c.Param("collectionId")
	var addParams AddParams
	if err := c.ShouldBindJSON(&addParams); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Handle request into collection
	err := benchmarkCollection.Put(addParams.Entries)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	c.JSON(http.StatusOK, gin.H{
		"colId":      collectionId,
		"count":      len(addParams.Entries),
		"firstEntry": fmt.Sprintf("%+v", addParams.Entries[0]),
	})
}

type SearchParams struct {
	Embedding []float32 `json:"embedding"`
	K         int       `json:"k"`
}

func collectionSearchHandler(c *gin.Context) {
	// collectionId := c.Param("collectionId")
	// ---------------------------
	var searchParams SearchParams
	if err := c.ShouldBindJSON(&searchParams); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Handle request into collection
	// ---------------------------
	nearestIds, err := benchmarkCollection.Search(searchParams.Embedding, searchParams.K)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	c.JSON(http.StatusOK, gin.H{
		"ids": nearestIds,
	})
}

// ---------------------------

func createRouter() *gin.Engine {
	router := gin.Default()
	v1 := router.Group("/v1")
	v1.GET("/ping", pongHandler)
	v1.POST("/collection", newCollectionHandler)
	v1.POST("/collection/:collectionId", collectionPutHandler)
	v1.POST("/collection/:collectionId/search", collectionSearchHandler)
	return router
}

// ---------------------------

func normalise(embedding []float32) {
	vector := blas32.Vector{N: len(embedding), Inc: 1, Data: embedding}
	norm := blas32.Nrm2(vector)
	blas32.Scal(1/norm, vector)
}

func loadHDF5(dataset string) {
	fname := fmt.Sprintf("data/%s.hdf5", dataset)
	log.Println("Loading dataset", fname)
	f, err := hdf5.OpenFile(fname, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	dset, err := f.OpenDataset("train")
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	dspace := dset.Space()
	dataBuf := make([]float32, dspace.SimpleExtentNPoints())
	if err := dset.Read(&dataBuf); err != nil {
		log.Fatal(err)
	}
	dims, _, err := dspace.SimpleExtentDims()
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	dset.Close()
	f.Close()
	// ---------------------------
	// matrix := numerical.Matrix{
	// 	Rows:   int(dims[0]),
	// 	Cols:   int(dims[1]),
	// 	Stride: int(dims[1]),
	// 	Data:   dataBuf,
	// }
	// fmt.Println(matrix.Cols)
	// ---------------------------
	log.Println("Creating entries", dims)
	entries := make([]collection.Entry, dims[0])
	for i := uint(0); i < dims[0]; i++ {
		embedding := dataBuf[i*dims[1] : (i+1)*dims[1]]
		if strings.Contains(dataset, "angular") {
			// Normalise embedding
			normalise(embedding)
		}
		entries[i] = collection.Entry{
			Id:        uint64(i),
			Embedding: dataBuf[i*dims[1] : (i+1)*dims[1]],
		}
	}
	// ---------------------------
	config := collection.DefaultCollectionConfig(dims[1])
	if strings.Contains(dataset, "angular") {
		config.DistMetric = "angular"
	}
	benchmarkCol, err := collection.NewCollection(config)
	if err != nil {
		log.Fatal(err)
	}
	// ---------------------------
	profileFile, _ := os.Create("dump/cpu.prof")
	defer profileFile.Close()
	pprof.StartCPUProfile(profileFile)
	defer pprof.StopCPUProfile()
	// ---------------------------
	if err := benchmarkCol.Put(entries); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cache size after insert", benchmarkCol.CacheSize())
	// ---------------------------
	fakeEntries := make([]collection.Entry, len(entries)/10)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len(fakeEntries); i++ {
		fakeEntries[i] = collection.Entry{
			Id:        uint64(rng.Intn(len(entries))),
			Embedding: make([]float32, dims[1]),
		}
		// Randomise embedding
		for j := 0; j < len(fakeEntries[i].Embedding); j++ {
			fakeEntries[i].Embedding[j] = rng.Float32()
			if strings.Contains(dataset, "angular") {
				// Normalise embedding
				normalise(fakeEntries[i].Embedding)
			}
		}
	}
	fakeIds := make(map[uint64]struct{}, len(fakeEntries))
	for _, entry := range fakeEntries {
		fakeIds[entry.Id] = struct{}{}
	}
	fmt.Println("Cache size before fake delete", benchmarkCol.CacheSize())
	if err := benchmarkCol.Delete(fakeIds); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cache size after fake delete", benchmarkCol.CacheSize())
	if err := benchmarkCol.Put(fakeEntries); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cache size after fake insert", benchmarkCol.CacheSize())
	realEntries := make([]collection.Entry, len(fakeEntries))
	for i := 0; i < len(fakeEntries); i++ {
		realEntries[i] = entries[fakeEntries[i].Id]
	}
	if err := benchmarkCol.Delete(fakeIds); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cache size after fake delete", benchmarkCol.CacheSize())
	if err := benchmarkCol.Put(realEntries); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Cache size after real insert", benchmarkCol.CacheSize())
	// ---------------------------
	numCycles := 1
	fmt.Println("Num cycles", numCycles)
	// 10 percent of entries
	deleteSize := int(0.2 * float64(len(entries)))
	fmt.Println("Delete size", deleteSize)
	// Set rng from current time
	for i := 0; i < numCycles; i++ {
		deleteSet := make(map[uint64]struct{}, deleteSize)
		deletedEntries := make([]collection.Entry, deleteSize)
		for j := 0; j < deleteSize; j++ {
			randId := uint64(rng.Intn(len(entries)))
			deleteSet[randId] = struct{}{}
			deletedEntries[j] = entries[randId]
		}
		fmt.Println("Deleting", len(deleteSet), "entries")
		if err := benchmarkCol.Delete(deleteSet); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Cache size after delete", benchmarkCol.CacheSize())
		// Re-insert
		if err := benchmarkCol.Put(deletedEntries); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("Cache size after delete re-insert cycles", benchmarkCol.CacheSize())
	// ---------------------------
	benchmarkCollection = benchmarkCol
	// ---------------------------
	// if err := collection.Close(); err != nil {
	// 	log.Fatal(err)
	// }
}

func runServer(router *gin.Engine) {
	// router.Run()
	// col, err := collection.OpenCollection("benchmark")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// benchmarkCollection = col
	// ---------------------------
	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	go func() {
		// service connections
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	// ---------------------------
	// Wait for interrupt signal to gracefully shutdown the server with
	// a timeout of 5 seconds.
	quit := make(chan os.Signal, 1)
	// kill (no param) default send syscanll.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall. SIGKILL but can"t be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}
	cancel()
	if err := benchmarkCollection.Close(); err != nil {
		log.Fatal(err)
	}
	// catching ctx.Done(). timeout of 5 seconds.
	<-ctx.Done()
	log.Println("Server exiting")
}

func main() {
	loadHDF5("glove-100-angular")
	// ---------------------------
	router := createRouter()
	runServer(router)
}
