package models

type Quantizer struct {
	Type    string                      `json:"type" binding:"required,oneof=none binary product"`
	Binary  *BinaryQuantizerParamaters  `json:"binary,omitempty"`
	Product *ProductQuantizerParameters `json:"product,omitempty"`
}

type BinaryQuantizerParamaters struct {
	// The threshold value for the binary quantizer. It is a pointer to distinguish
	// between 0 value vs not set.
	Threshold *float32 `json:"threshold"`
	// Number of points to use to calculate the threshold. It may be ignored if
	// the threshold is set.
	TriggerThreshold int `json:"triggerThreshold" binding:"min=0,max=50000"`
	// Distance function to use for binary quantizer, it can be either hamming or jaccard.
	DistanceMetric string `json:"distanceMetric" binding:"required,oneof=hamming jaccard"`
}

type ProductQuantizerParameters struct {
	// Number of centroids to quantize to, this is the k* parameter in the paper
	// and is often set to 255 giving 256 centroids (including 0). We are
	// limiting this to maximum of 256 (uint8) to keep the overhead of this
	// process tractable.
	NumCentroids int `json:"numCentroids" binding:"required,min=2,max=256"`
	// Number of subvectors / segments / subquantizers to use, this is the m
	// parameter in the paper and is often set to 8.
	NumSubVectors int `json:"numSubVectors" binding:"required,min=2"`
	// Number of points to use to train the quantizer, it will automatically trigger training
	// when this number of points is reached.
	TriggerThreshold int `json:"triggerThreshold" binding:"required,min=1000,max=10000"`
}
