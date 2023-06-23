package models

import "github.com/google/uuid"

type Collection struct {
	Id         uuid.UUID
	Name       string
	EmbedSize  uint
	DistMetric string
	Owner      string
	Package    string
	Algorithm  string
}

type VamanaParameters struct {
	SearchSize  int
	DegreeBound int
	Alpha       float32
}

func DefaultVamanaParameters() VamanaParameters {
	return VamanaParameters{
		SearchSize:  75,
		DegreeBound: 64,
		Alpha:       1.2,
	}
}

type VamanaCollection struct {
	Collection
	Parameters VamanaParameters
}
