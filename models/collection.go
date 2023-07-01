package models

type Collection struct {
	Id         string
	EmbedSize  uint
	DistMetric string
	Shards     uint
	Replicas   uint
	Algorithm  string
	Version    int64
	CreatedAt  int64
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
