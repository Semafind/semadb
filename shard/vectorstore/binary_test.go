package vectorstore

import (
	"testing"

	"github.com/semafind/semadb/diskstore"
	"github.com/semafind/semadb/models"
	"github.com/stretchr/testify/require"
)

func Test_Binary_Encode(t *testing.T) {
	var threshold float32 = 0.5
	params := models.BinaryQuantizerParamaters{
		Threshold:      &threshold,
		DistanceMetric: models.DistanceHamming,
	}
	bq, err := newBinaryQuantizer(nil, nil, params, 5)
	require.NoError(t, err)
	vector := []float32{1.0, 0.1, 0.6, 0.7, 0.4}
	encoded := bq.encode(vector)
	require.Len(t, encoded, 1)
	require.Equal(t, uint64(0b01101), encoded[0])
}

func Test_Binary_Fit(t *testing.T) {
	params := models.BinaryQuantizerParamaters{
		TriggerThreshold: 2,
		DistanceMetric:   models.DistanceHamming,
	}
	bq, err := newBinaryQuantizer(diskstore.NewMemBucket(false), nil, params, 2)
	require.NoError(t, err)
	_, err = bq.Set(1, []float32{1.0, 2.0})
	require.NoError(t, err)
	_, err = bq.Set(2, []float32{3.0, 4.0})
	require.NoError(t, err)
	require.NoError(t, bq.Fit())
	// Check if the threshold is the mean
	require.Equal(t, []float32{2.0, 3.0}, bq.threshold)
}
