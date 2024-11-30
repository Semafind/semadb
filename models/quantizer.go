package models

import "fmt"

type Quantizer struct {
	Type    string                      `json:"type" binding:"required,oneof=none binary product"`
	Binary  *BinaryQuantizerParamaters  `json:"binary,omitempty"`
	Product *ProductQuantizerParameters `json:"product,omitempty"`
}

func (q Quantizer) Validate() error {
	switch q.Type {
	case QuantizerNone:
		return nil
	case QuantizerBinary:
		if q.Binary == nil {
			return fmt.Errorf("binary quantizer parameters not provided")
		}
		return q.Binary.Validate()
	case QuantizerProduct:
		if q.Product == nil {
			return fmt.Errorf("product quantizer parameters not provided")
		}
		return q.Product.Validate()
	default:
		return fmt.Errorf("unknown quantizer type %s", q.Type)
	}
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

func (b BinaryQuantizerParamaters) Validate() error {
	if b.Threshold == nil && (b.TriggerThreshold < 0 || b.TriggerThreshold > 50000) {
		return fmt.Errorf("triggerThreshold must be between 0 and 50000, got %d", b.TriggerThreshold)
	}
	if b.DistanceMetric != DistanceHamming && b.DistanceMetric != DistanceJaccard {
		return fmt.Errorf("invalid distance metric for binary quantization, got %s", b.DistanceMetric)
	}
	return nil
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

func (p ProductQuantizerParameters) Validate() error {
	if p.NumCentroids < 2 || p.NumCentroids > 256 {
		return fmt.Errorf("numCentroids must be between 2 and 256, got %d", p.NumCentroids)
	}
	if p.NumSubVectors < 2 {
		return fmt.Errorf("numSubVectors must be at least 2, got %d", p.NumSubVectors)
	}
	if p.TriggerThreshold < 1000 || p.TriggerThreshold > 10000 {
		return fmt.Errorf("triggerThreshold must be between 1000 and 10000, got %d", p.TriggerThreshold)
	}
	return nil
}
