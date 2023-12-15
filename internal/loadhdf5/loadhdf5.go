package loadhdf5

import (
	"math"
	"path/filepath"
	"strings"

	"gonum.org/v1/hdf5"
)

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

func LoadHDF5(fpath string) (VectorCollection, error) {
	vCol := VectorCollection{Name: filepath.Base(fpath)}
	f, err := hdf5.OpenFile(fpath, hdf5.F_ACC_RDONLY)
	if err != nil {
		return vCol, err
	}
	// ---------------------------
	dset, err := f.OpenDataset("train")
	if err != nil {
		return vCol, err
	}
	// ---------------------------
	dspace := dset.Space()
	dataBuf := make([]float32, dspace.SimpleExtentNPoints())
	if err := dset.Read(&dataBuf); err != nil {
		return vCol, err
	}
	dims, _, err := dspace.SimpleExtentDims()
	if err != nil {
		return vCol, err
	}
	// ---------------------------
	dset.Close()
	f.Close()
	// ---------------------------
	vectors := make([][]float32, dims[0])
	for i := uint(0); i < dims[0]; i++ {
		vectors[i] = dataBuf[i*dims[1] : (i+1)*dims[1]]
		if strings.Contains(fpath, "angular") {
			// Normalise embedding
			normalise(vectors[i])
		}
	}
	vCol.Vectors = vectors
	// ---------------------------
	vCol.DistMetric = "euclidean"
	if strings.Contains(fpath, "angular") {
		vCol.DistMetric = "cosine"
	}
	// ---------------------------
	return vCol, nil
}
