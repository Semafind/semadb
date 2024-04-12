package models

import (
	"fmt"
)

// Defines the index schema for a collection, each index type is a map of property names
// to index parameters. The index parameters are different for each index type.
type IndexSchema map[string]IndexSchemaValue

func (s IndexSchema) Validate() error {
	for k, v := range s {
		switch v.Type {
		case IndexTypeVectorFlat:
			if v.VectorFlat == nil {
				return fmt.Errorf("vectorFlat parameters not provided for property %s", k)
			}
			if v.VectorFlat.DistanceMetric == DistanceHaversine && v.VectorFlat.VectorSize != 2 {
				return fmt.Errorf("haversine distance metric requires vector size 2 for property %s, got %d", k, v.VectorFlat.VectorSize)
			}
		case IndexTypeVectorVamana:
			if v.VectorVamana == nil {
				return fmt.Errorf("vectorVamana parameters not provided for property %s", k)
			}
			if v.VectorVamana.DistanceMetric == DistanceHaversine && v.VectorVamana.VectorSize != 2 {
				return fmt.Errorf("haversine distance metric requires vector size 2 for property %s, got %d", k, v.VectorVamana.VectorSize)
			}
		case IndexTypeText:
			if v.Text == nil {
				return fmt.Errorf("text parameters not provided for property %s", k)
			}
		case IndexTypeString:
			if v.String == nil {
				return fmt.Errorf("string parameters not provided for property %s", k)
			}
		case IndexTypeStringArray:
			if v.StringArray == nil {
				return fmt.Errorf("stringArray parameters not provided for property %s", k)
			}
		case IndexTypeInteger:
			// Nothing to check
		case IndexTypeFloat:
		default:
			return fmt.Errorf("unknown index type %s", v.Type)
		}
	}
	return nil
}

type IndexSchemaValue struct {
	Type         string                       `json:"type" binding:"required,oneof=vectorFlat vectorVamana text string integer float stringArray"`
	VectorFlat   *IndexVectorFlatParameters   `json:"vectorFlat,omitempty"`
	VectorVamana *IndexVectorVamanaParameters `json:"vectorVamana,omitempty"`
	Text         *IndexTextParameters         `json:"text,omitempty"`
	String       *IndexStringParameters       `json:"string,omitempty"`
	StringArray  *IndexStringArrayParameters  `json:"stringArray,omitempty"`
}

// Attempts to convert a given value to a vector
func convertToVector(v any) ([]float32, error) {
	// This mess happens because we are dealing with arbitrary JSON.
	// Nothing stops the user from passing "vector": "memes" as valid
	// JSON. Furthermore, JSON by default decodes floats to float64.
	var vector []float32
	switch v := v.(type) {
	case []float32:
		return v, nil
	case []float64:
		vector = make([]float32, len(v))
		for i, f := range v {
			vector[i] = float32(f)
		}
		return vector, nil
	case []any:
		vector = make([]float32, len(v))
		for i, f := range v {
			switch f := f.(type) {
			case float32:
				vector[i] = f
			case float64:
				vector[i] = float32(f)
			default:
				return nil, fmt.Errorf("expected float, got %T", f)
			}
		}
	default:
		return nil, fmt.Errorf("expected vector array, got %T", v)
	}
	return vector, nil
}

// Check if a given map is compatible with the index schema
func (s IndexSchema) CheckCompatibleMap(m PointAsMap) error {
	// We will go through each index field, check if the map has them, is of
	// right type and any extra checks needed
	// ---------------------------
	// For example, k="age" and v=42
	for k, v := range m {
		schema, ok := s[k]
		if !ok {
			// This property is not in the indexed schema
			continue
		}
		switch schema.Type {
		case IndexTypeVectorFlat:
			vector, err := convertToVector(v)
			if err != nil {
				return fmt.Errorf("expected a vector for property %s: %w", k, err)
			}
			if schema.VectorFlat == nil {
				return fmt.Errorf("vectorFlat parameters not provided for %s", k)
			}
			if len(vector) != int(schema.VectorFlat.VectorSize) {
				return fmt.Errorf("expected vector of size %d for property %s, got %d", schema.VectorFlat.VectorSize, k, len(vector))
			}
			// We override the map value with the vector so downstream code can
			// use the vector directly.
			m[k] = vector
		case IndexTypeVectorVamana:
			vector, err := convertToVector(v)
			if err != nil {
				return fmt.Errorf("expected a vector for property %s: %w", k, err)
			}
			if schema.VectorVamana == nil {
				return fmt.Errorf("vamanaVamana parameters not provided for %s", k)
			}
			if len(vector) != int(schema.VectorVamana.VectorSize) {
				return fmt.Errorf("expected vector of size %d for property %s, got %d", schema.VectorVamana.VectorSize, k, len(vector))
			}
			// We override the map value with the vector so downstream code can
			// use the vector directly.
			m[k] = vector
		case IndexTypeText:
			fallthrough
		case IndexTypeString:
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected string for property %s, got %T", k, v)
			}
		case IndexTypeInteger:
			switch v := v.(type) {
			case int64:
			case int:
				m[k] = int64(v)
			case int32:
				m[k] = int64(v)
			case uint:
				m[k] = int64(v)
			case uint32:
				m[k] = int64(v)
			// Floating point cases are here because encoding/json decodes
			// any number as float64. So you give it say 42, and it gives
			// you back float64(42)
			case float32:
				m[k] = int64(v)
			case float64:
				m[k] = int64(v)
			// We are not supporting uint64 because it won't fit in int64
			// case uint64:
			default:
				return fmt.Errorf("expected integer number for property %s, got %T", k, v)
			}
		case IndexTypeFloat:
			switch v := v.(type) {
			case float64:
			case float32:
				m[k] = float64(v)
			default:
				return fmt.Errorf("expected floating point number for property %s, got %T", k, v)
			}
		case IndexTypeStringArray:
			switch v := v.(type) {
			case []string:
			case []any:
				// Similar problem to float vectors, encoding/json gives back []any for arrays
				strs := make([]string, len(v))
				for i, s := range v {
					if ss, ok := s.(string); ok {
						strs[i] = ss
					} else {
						return fmt.Errorf("expected string array for property %s, got %T", k, s)
					}
				}
				m[k] = strs
			default:
				return fmt.Errorf("expected string array for property %s, got %T", k, v)
			}
		}
	}
	// ---------------------------
	return nil
}

type IndexVectorFlatParameters struct {
	VectorSize     uint       `json:"vectorSize" binding:"required,min=1,max=4096"`
	DistanceMetric string     `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot hamming jaccard haversine"`
	Quantizer      *Quantizer `json:"quantizer,omitempty"`
}

type IndexVectorVamanaParameters struct {
	VectorSize     uint       `json:"vectorSize" binding:"required,min=1,max=4096"`
	DistanceMetric string     `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot hamming jaccard haversine"`
	SearchSize     int        `json:"searchSize" binding:"min=25,max=75"`
	DegreeBound    int        `json:"degreeBound" binding:"min=32,max=64"`
	Alpha          float32    `json:"alpha" binding:"min=1.1,max=1.5"`
	Quantizer      *Quantizer `json:"quantizer,omitempty"`
}

type IndexTextParameters struct {
	Analyser string `json:"analyser" binding:"required,oneof=standard"`
}

type IndexStringParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}

type IndexStringArrayParameters struct {
	IndexStringParameters
}
