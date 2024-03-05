package models

type Collection struct {
	UserId     string
	Id         string
	VectorSize uint
	DistMetric string
	Replicas   uint
	Algorithm  string
	Timestamp  int64
	CreatedAt  int64
	ShardIds   []string
	Parameters VamanaParameters
	// Active user plan
	UserPlan    UserPlan
	IndexSchema IndexSchema
}

/* The IndexSchema is defined as different fields of index types to save us from
 * parsing arbitrary JSON. The original design was to include an object with
 * {type: indexType} and {parameters: indexParameters} fields. We quickly
 * realised this would create downstream parsing code. Although the explicit
 * approach may be verbose and may degrade user experience, it is far less error
 * prone due to strong typing. */

// Defines the index schema for a collection, each index type is a map of property names
// to index parameters. The index parameters are different for each index type.
type IndexSchema struct {
	VectorFlat   map[string]IndexVectorFlatParameters   `json:"vectorFlat" binding:"dive"`
	VectorVamana map[string]IndexVectorVamanaParameters `json:"vectorVamana" binding:"dive"`
	Text         map[string]IndexTextParameters         `json:"text" binding:"dive"`
	String       map[string]IndexStringParameters       `json:"string" binding:"dive"`
	Integer      map[string]struct{}                    `json:"integer"`
	Float        map[string]struct{}                    `json:"float"`
	StringArray  map[string]struct{}                    `json:"stringArray"`
}

// Checks if there are duplicate property names in the index schema
func (s *IndexSchema) CheckDuplicatePropValue() (string, bool) {
	seen := make(map[string]struct{})
	for _, m := range []map[string]struct{}{s.Integer, s.Float, s.StringArray} {
		for k := range m {
			if _, ok := seen[k]; ok {
				return k, true
			}
			seen[k] = struct{}{}
		}
	}
	return "", false
}

type IndexVectorFlatParameters struct {
	VectorSize     uint   `json:"vectorSize" binding:"required,min=1,max=4096"`
	DistanceMetric string `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot"`
}

type IndexVectorVamanaParameters struct {
	IndexVectorFlatParameters
	SearchSize  int     `json:"searchSize" binding:"min=25,max=75"`
	DegreeBound int     `json:"degreeBound" binding:"min=32,max=64"`
	Alpha       float32 `json:"alpha" binding:"min=1.1,max=1.5"`
}

type IndexTextParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}

type VamanaParameters struct {
	SearchSize  int
	DegreeBound int
	Alpha       float32
}

type IndexStringParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}

func DefaultVamanaParameters() VamanaParameters {
	return VamanaParameters{
		SearchSize:  75,
		DegreeBound: 64,
		Alpha:       1.2,
	}
}
