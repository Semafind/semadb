package main

import (
	"fmt"
	"log"
	"net/http"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
	"github.com/semafind/semadb/collection"
	"gonum.org/v1/hdf5"
)

// ---------------------------

func pongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong from semadb",
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
	db, err := badger.Open(badger.DefaultOptions("dump/" + collectionId))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	collection := collection.NewCollection(collectionId, db)
	err = collection.Put(addParams.Entries)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := db.Close(); err != nil {
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
	collectionId := c.Param("collectionId")
	// ---------------------------
	var searchParams SearchParams
	if err := c.ShouldBindJSON(&searchParams); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	// Handle request into collection
	db, err := badger.Open(badger.DefaultOptions("dump/" + collectionId))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	collection := collection.NewCollection(collectionId, db)
	// ---------------------------
	result, err := collection.Search(searchParams.Embedding, searchParams.K)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// ---------------------------
	c.JSON(http.StatusOK, gin.H{
		"result": fmt.Sprintf("%+v", result),
	})
}

// ---------------------------

func runServer() {
	router := gin.Default()
	v1 := router.Group("/v1")
	v1.GET("/ping", pongHandler)
	v1.POST("/collection/:collectionId", collectionPutHandler)
	v1.POST("/collection/:collectionId/search", collectionSearchHandler)
	router.Run()
}

// ---------------------------

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
		entries[i] = collection.Entry{
			Id:        fmt.Sprint(i),
			Embedding: dataBuf[i*dims[1] : (i+1)*dims[1]],
		}
	}
	// ---------------------------
	db, err := badger.Open(badger.DefaultOptions("dump/" + dataset))
	if err != nil {
		log.Fatal(err)
	}
	collection := collection.NewCollection(dataset, db)
	// ---------------------------
	if err := collection.Put(entries[:100000]); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	// runServer()
	loadHDF5("glove-25-angular")
}
