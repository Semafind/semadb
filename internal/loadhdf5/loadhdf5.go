package main

import (
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gonum.org/v1/hdf5"
)

type NewPointRequest struct {
	Vector   []float32 `json:"vector" binding:"required"`
	Metadata any       `json:"metadata"`
}

func normalise(embedding []float32) {
	// ---------------------------
	// Normalise vector
	var magnitude float32 = 0.0
	for _, v := range embedding {
		magnitude += v * v
	}
	magnitude = float32(math.Sqrt(float64(magnitude)))
	for i, v := range embedding {
		embedding[i] = v / magnitude
	}
}

func loadHDF5(dataset string) {
	fname := fmt.Sprintf("data/%s.hdf5", dataset)
	log.Info().Str("fname", fname).Msg("loadHDF5")
	f, err := hdf5.OpenFile(fname, hdf5.F_ACC_RDONLY)
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5")
	}
	// ---------------------------
	dset, err := f.OpenDataset("train")
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5")
	}
	// ---------------------------
	dspace := dset.Space()
	dataBuf := make([]float32, dspace.SimpleExtentNPoints())
	if err := dset.Read(&dataBuf); err != nil {
		log.Fatal().Err(err).Msg("loadHDF5")
	}
	dims, _, err := dspace.SimpleExtentDims()
	if err != nil {
		log.Fatal().Err(err).Msg("loadHDF5")
	}
	// ---------------------------
	dset.Close()
	f.Close()
	// ---------------------------
	log.Debug().Uint("dims[0]", dims[0]).Uint("dims[1]", dims[1]).Msg("loadHDF5")
	points := make([]NewPointRequest, dims[0])
	for i := uint(0); i < dims[0]; i++ {
		embedding := dataBuf[i*dims[1] : (i+1)*dims[1]]
		if strings.Contains(dataset, "angular") {
			// Normalise embedding
			normalise(embedding)
		}
		points[i] = NewPointRequest{
			Vector: dataBuf[i*dims[1] : (i+1)*dims[1]],
			Metadata: map[string]interface{}{
				"xid": i,
			},
		}
	}
	// ---------------------------
	distMetric := "euclidean"
	if strings.Contains(dataset, "angular") {
		distMetric = "cosine"
	}
	// ---------------------------
	// Create a new collection
	collectionName := strings.ReplaceAll(dataset, "-", "")
	createCollection(collectionName, dims[1], distMetric)
	// ---------------------------
	batchSize := 1000
	for i := 0; i < len(points); i += batchSize {
		end := i + batchSize
		if end > len(points) {
			end = len(points)
		}
		// ---------------------------
		// Add points to collection
		log.Debug().Int("i", i).Int("end", end).Msg("loadHDF5 - createPoints")
		if err := createPoints(collectionName, points[i:end]); err != nil {
			log.Fatal().Err(err).Msg("loadHDF5")
		}
		if i > 2 {
			break
		}
	}
}

func main() {
	// Pretty print logs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	// ---------------------------
	loadHDF5("glove-25-angular")
}
