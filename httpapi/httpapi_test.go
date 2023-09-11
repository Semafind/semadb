package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/semafind/semadb/cluster"
	"github.com/stretchr/testify/assert"
)

func setupClusterNode(t *testing.T) *cluster.ClusterNode {
	cnode, err := cluster.NewNode(cluster.ClusterNodeConfig{
		RootDir: t.TempDir(),
		Servers: []string{"localhost:9898"},
		// ---------------------------
		RpcHost:    "localhost",
		RpcPort:    9898,
		RpcTimeout: 5,
		RpcRetries: 2,
		// ---------------------------
		ShardTimeout:       30,
		MaxShardSize:       268435456, // 2GiB
		MaxShardPointCount: 250000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cnode
}

func setupTestRouter(t *testing.T) *gin.Engine {
	httpConfig := HttpApiConfig{
		Debug:    true,
		HttpHost: "localhost",
		HttpPort: 8081,
		UserPlans: map[string]UserPlan{
			"BASIC": {
				Name:              "BASIC",
				MaxCollections:    1,
				MaxCollectionSize: 1073741824, // 1GiB
				MaxMetadataSize:   1024,
			},
		},
	}
	return setupRouter(setupClusterNode(t), httpConfig)
}

func Test_pongHandler(t *testing.T) {
	router := setupTestRouter(t)
	req, err := http.NewRequest("GET", "/v1/ping", nil)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	// With App Headers
	req.Header.Set("X-User-Id", "testy")
	req.Header.Set("X-Package", "BASIC")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "{\"message\":\"pong from semadb\"}", w.Body.String())
}
