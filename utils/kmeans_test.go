package utils_test

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/semafind/semadb/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKMeans_Fit(t *testing.T) {
	// ---------------------------
	dataOffsets := [][]float32{
		{-1, -1, 1, 1},
		{-1, -1, 1, 1},
		{0, 0, -1, 1},
		{0, 0, -1, 1},
		{1, 1, 1, -1},
		{1, 1, 1, -1},
	}
	data := make([][]float32, len(dataOffsets))
	for i, offsets := range dataOffsets {
		data[i] = make([]float32, len(offsets))
		for j, v := range offsets {
			data[i][j] = v*10 + rand.Float32()
		}
	}
	// ---------------------------
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		kmeans := utils.KMeans{
			K:         3,
			MaxIter:   10,
			Offset:    0,
			VectorLen: 2,
		}
		kmeans.Fit(data)
		assert.Equal(t, 3, len(kmeans.Centroids))
		assert.Equal(t, kmeans.Labels[0], kmeans.Labels[1])
		assert.NotEqual(t, kmeans.Labels[0], kmeans.Labels[2])
		assert.Equal(t, kmeans.Labels[2], kmeans.Labels[3])
		assert.NotEqual(t, kmeans.Labels[2], kmeans.Labels[4])
		assert.Equal(t, kmeans.Labels[4], kmeans.Labels[5])
		wg.Done()
	}()
	go func() {
		kmeans := utils.KMeans{
			K:         3,
			MaxIter:   10,
			Offset:    2,
			VectorLen: 2,
		}
		kmeans.Fit(data)
		assert.Equal(t, 3, len(kmeans.Centroids))
		assert.Equal(t, kmeans.Labels[0], kmeans.Labels[1])
		assert.NotEqual(t, kmeans.Labels[0], kmeans.Labels[2])
		assert.Equal(t, kmeans.Labels[2], kmeans.Labels[3])
		assert.NotEqual(t, kmeans.Labels[2], kmeans.Labels[4])
		assert.Equal(t, kmeans.Labels[4], kmeans.Labels[5])
		wg.Done()
	}()
	wg.Wait()
}

func TestKMeans_Large(t *testing.T) {
	// ---------------------------
	dataSize := 10000
	vectorSize := 16
	// ---------------------------
	data := make([][]float32, dataSize)
	for i := 0; i < dataSize; i++ {
		data[i] = make([]float32, vectorSize)
		for j := 0; j < vectorSize; j++ {
			data[i][j] = rand.Float32()
		}
	}
	// ---------------------------
	kmeans := utils.KMeans{
		K:         256,
		MaxIter:   1000,
		Offset:    0,
		VectorLen: vectorSize,
	}
	kmeans.Fit(data)
	require.Equal(t, 256, len(kmeans.Centroids))
}

func BenchmarkKMeans_Fit(b *testing.B) {
	datasetSize := []int{1000, 10000}
	vectorSize := []int{4, 8, 16}
	// Disable zerolog
	zerolog.SetGlobalLevel(zerolog.Disabled)
	for _, ds := range datasetSize {
		for _, vs := range vectorSize {
			benchmarkName := fmt.Sprintf("DatasetSize=%d-VectorSize=%d", ds, vs)
			b.Run(benchmarkName, func(b *testing.B) {
				data := make([][]float32, ds)
				for i := 0; i < ds; i++ {
					data[i] = make([]float32, vs)
					for j := 0; j < vs; j++ {
						data[i][j] = rand.Float32()
					}
				}
				kmeans := utils.KMeans{
					K:         255,
					MaxIter:   1000,
					Offset:    0,
					VectorLen: vs,
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					kmeans.Fit(data)
				}
			})
		}
	}
}
