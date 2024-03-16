package models

import "fmt"

type Collection struct {
	UserId    string
	Id        string
	Replicas  uint
	Timestamp int64
	CreatedAt int64
	ShardIds  []string
	// Active user plan
	UserPlan    UserPlan
	IndexSchema IndexSchema
}

const (
	IndexTypeVectorFlat   = "vectorFlat"
	IndexTypeVectorVamana = "vectorVamana"
	IndexTypeText         = "text"
	IndexTypeString       = "string"
	IndexTypeInteger      = "integer"
	IndexTypeFloat        = "float"
	IndexTypeStringArray  = "stringArray"
)

// Defines the index schema for a collection, each index type is a map of property names
// to index parameters. The index parameters are different for each index type.
type IndexSchema map[string]IndexSchemaValue

func (s IndexSchema) Validate() error {
	for k, v := range s {
		switch v.Type {
		case IndexTypeVectorFlat:
			if v.VectorFlat == nil {
				return fmt.Errorf("vectorFlat parameters not provided for %s", k)
			}
		case IndexTypeVectorVamana:
			if v.VectorVamana == nil {
				return fmt.Errorf("vectorVamana parameters not provided for %s", k)
			}
		case IndexTypeText:
			if v.Text == nil {
				return fmt.Errorf("text parameters not provided for %s", k)
			}
		case IndexTypeString:
			if v.String == nil {
				return fmt.Errorf("string parameters not provided for %s", k)
			}
		case IndexTypeInteger:
			if v.Integer != nil {
				return fmt.Errorf("integer parameters not provided for %s", k)
			}
		case IndexTypeFloat:
			if v.Float != nil {
				return fmt.Errorf("float parameters not provided for %s", k)
			}
		case IndexTypeStringArray:
			if v.StringArray != nil {
				return fmt.Errorf("stringArray parameters not provided for %s", k)
			}
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
	Integer      *struct{}                    `json:"integer,omitempty"`
	Float        *struct{}                    `json:"float,omitempty"`
	StringArray  *struct{}                    `json:"stringArray,omitempty"`
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
				return nil, fmt.Errorf("expected float32, got %T", f)
			}
		}
	default:
		return nil, fmt.Errorf("expected vector, got %T", v)
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
				return fmt.Errorf("expected vector for %s, got %T", k, v)
			}
			if schema.VectorFlat == nil {
				return fmt.Errorf("vectorFlat parameters not provided for %s", k)
			}
			if len(vector) != int(schema.VectorFlat.VectorSize) {
				return fmt.Errorf("expected vector of size %d for %s, got %d", schema.VectorFlat.VectorSize, k, len(vector))
			}
			// We override the map value with the vector so downstream code can
			// use the vector directly.
			m[k] = vector
		case IndexTypeVectorVamana:
			vector, err := convertToVector(v)
			if err != nil {
				return fmt.Errorf("expected vector for %s, got %T", k, v)
			}
			if schema.VectorVamana == nil {
				return fmt.Errorf("vamana parameters not provided for %s", k)
			}
			if len(vector) != int(schema.VectorVamana.VectorSize) {
				return fmt.Errorf("expected vector of size %d for %s, got %d", schema.VectorVamana.VectorSize, k, len(vector))
			}
			// We override the map value with the vector so downstream code can
			// use the vector directly.
			m[k] = vector
		case IndexTypeText:
			fallthrough
		case IndexTypeString:
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected string for %s, got %T", k, v)
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
			// We are not supporting uint64 because it won't fit in int64
			// case uint64:
			default:
				return fmt.Errorf("expected int for %s, got %T", k, v)
			}
		case IndexTypeFloat:
			switch v := v.(type) {
			case float32:
			case float64:
				m[k] = float32(v)
			default:
				return fmt.Errorf("expected float for %s, got %T", k, v)
			}
		case IndexTypeStringArray:
			if _, ok := v.([]string); !ok {
				return fmt.Errorf("expected string array for %s, got %T", k, v)
			}
		}
	}
	// ---------------------------
	return nil
}

type IndexVectorFlatParameters struct {
	VectorSize     uint   `json:"vectorSize" binding:"required,min=1,max=4096"`
	DistanceMetric string `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot"`
}

type IndexVectorVamanaParameters struct {
	VectorSize     uint    `json:"vectorSize" binding:"required,min=1,max=4096"`
	DistanceMetric string  `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot"`
	SearchSize     int     `json:"searchSize" binding:"min=25,max=75"`
	DegreeBound    int     `json:"degreeBound" binding:"min=32,max=64"`
	Alpha          float32 `json:"alpha" binding:"min=1.1,max=1.5"`
}

type IndexTextParameters struct {
	Analyser string `json:"analyser" binding:"required,oneof=standard"`
}

type IndexStringParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}
