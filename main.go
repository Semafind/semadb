package main

import (
	"fmt"
	"net/http"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gin-gonic/gin"
	"github.com/semafind/semadb/collection"
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

func main() {
	runServer()
}
