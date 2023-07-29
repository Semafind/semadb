package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/semafind/semadb/cluster"
	"github.com/semafind/semadb/config"
	"github.com/semafind/semadb/models"
	"github.com/semafind/semadb/shard"
	"github.com/vmihailenco/msgpack/v5"
	"gonum.org/v1/hdf5"
)

type NewPointRequest struct {
	Vector   []float32 `json:"vector" binding:"required"`
	Metadata any       `json:"metadata"`
}

type VectorCollection struct {
	Name       string      `json:"name"`
	Vectors    [][]float32 `json:"vectors"`
	DistMetric string      `json:"distMetric"`
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

func loadHDF5(dataset string) VectorCollection {
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
	log.Debug().Uint("dims[0]", dims[0]).Uint("dims[1]", dims[1]).Msg("loadHDF5")
	// ---------------------------
	vectors := make([][]float32, dims[0])
	for i := uint(0); i < dims[0]; i++ {
		vectors[i] = dataBuf[i*dims[1] : (i+1)*dims[1]]
		if strings.Contains(dataset, "angular") {
			// Normalise embedding
			normalise(vectors[i])
		}
	}
	// ---------------------------
	distMetric := "euclidean"
	if strings.Contains(dataset, "angular") {
		distMetric = "cosine"
	}
	// ---------------------------
	return VectorCollection{
		Name:       strings.ReplaceAll(dataset, "-", ""),
		Vectors:    vectors,
		DistMetric: distMetric,
	}
}

func loadRemote(vcol VectorCollection) {
	createCollection(vcol.Name, len(vcol.Vectors[0]), vcol.DistMetric)
	// ---------------------------
	batchSize := 100000
	reqPoints := make([]NewPointRequest, batchSize)
	for i := 0; i < len(vcol.Vectors); i += batchSize {
		end := i + batchSize
		if end > len(vcol.Vectors) {
			end = len(vcol.Vectors)
		}
		// ---------------------------
		reqPoints = reqPoints[:end-i]
		for j := i; j < end; j++ {
			reqPoints[j-i] = NewPointRequest{
				Vector: vcol.Vectors[j],
				Metadata: map[string]interface{}{
					"xid": i,
				},
			}
		}
		// ---------------------------
		// Add points to collection
		log.Debug().Int("i", i).Int("end", end).Msg("loadHDF5 - createPoints")
		if err := createPoints(vcol.Name, reqPoints); err != nil {
			log.Fatal().Err(err).Msg("loadHDF5")
		}
	}
}

func loadIntoShard(vcol VectorCollection) {
	points := make([]models.Point, len(vcol.Vectors))
	for i := 0; i < len(vcol.Vectors); i++ {
		mdata, _ := msgpack.Marshal(map[string]interface{}{
			"xid": i})
		points[i] = models.Point{
			Id:       uuid.New(),
			Vector:   vcol.Vectors[i],
			Metadata: mdata,
		}
	}
	// ---------------------------
	// Create a new collection
	collection := models.Collection{
		UserId:     "benchmark",
		Id:         vcol.Name,
		VectorSize: uint(len(vcol.Vectors[0])),
		DistMetric: vcol.DistMetric,
		Replicas:   1,
		Algorithm:  "vamana",
		Timestamp:  0,
		CreatedAt:  0,
		Parameters: models.DefaultVamanaParameters(),
	}
	// ---------------------------
	clusterNode := &cluster.ClusterNode{}
	config.Cfg.RootDir = "dump"
	if err := clusterNode.CreateCollection(collection); err != nil {
		log.Fatal().Err(err).Msg("loadHDF5")
	}
	// ---------------------------
	shardId := uuid.New().String()
	shardDir := filepath.Join("dump", "benchmark", vcol.Name, shardId)
	os.MkdirAll(shardDir, os.ModePerm)
	shard, _ := shard.NewShard(shardDir, collection)
	log.Info().Str("shardId", shardId).Str("shardDir", shardDir).Msg("loadHDF5")
	// ---------------------------
	batchSize := 100000
	for i := 0; i < len(points); i += batchSize {
		end := i + batchSize
		if end > len(points) {
			end = len(points)
		}
		// ---------------------------
		// Add points to collection
		log.Debug().Int("i", i).Int("end", end).Msg("loadHDF5 - createPoints")
		if _, err := shard.UpsertPoints(points[i:end]); err != nil {
			log.Fatal().Err(err).Msg("loadHDF5")
		}
	}
}

func main() {
	// Pretty print logs
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	// ---------------------------
	vcol := loadHDF5("glove-25-angular")
	// ---------------------------
	isRemote := flag.Bool("remote", false, "load into remote collection")
	flag.Parse()
	if *isRemote {
		log.Info().Msg("loadHDF5 - remote")
		loadRemote(vcol)
	} else {
		log.Info().Msg("loadHDF5 - shard")
		loadIntoShard(vcol)
	}
}