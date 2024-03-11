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

/* The IndexSchema is defined as different fields of index types to save us from
 * parsing arbitrary JSON. The original design was to include an object with
 * {type: indexType, parameters: indexParameters} fields. We quickly
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

type indexProperty struct {
	Name string
	Type string
}

func (s *IndexSchema) CollectAllProperties() []indexProperty {
	var properties []indexProperty
	// ---------------------------
	// Do NOT change type values without handling migration. The type values are
	// used in bucket names, e.g. index/vectorVamana/[property name]
	// ---------------------------
	for k := range s.VectorFlat {
		properties = append(properties, indexProperty{Name: k, Type: "vectorFlat"})
	}
	for k := range s.VectorVamana {
		properties = append(properties, indexProperty{Name: k, Type: "vectorVamana"})
	}
	for k := range s.Text {
		properties = append(properties, indexProperty{Name: k, Type: "text"})
	}
	for k := range s.String {
		properties = append(properties, indexProperty{Name: k, Type: "string"})
	}
	for k := range s.Integer {
		properties = append(properties, indexProperty{Name: k, Type: "integer"})
	}
	for k := range s.Float {
		properties = append(properties, indexProperty{Name: k, Type: "float"})
	}
	for k := range s.StringArray {
		properties = append(properties, indexProperty{Name: k, Type: "stringArray"})
	}
	return properties
}

// Check if a given map is compatible with the index schema
func (s *IndexSchema) CheckCompatibleMap(m map[string]any) error {
	// We will go through each index field, check if the map has them, is of
	// right type and any extra checks needed
	// ---------------------------
	for k, params := range s.VectorFlat {
		if v, ok := m[k]; ok {
			vector, ok := v.([]float32)
			if !ok {
				return fmt.Errorf("expected vector for property %s, got %T", k, v)
			}
			if len(vector) != int(params.VectorSize) {
				return fmt.Errorf("expected vector of size %d for %s, got %d", params.VectorSize, k, len(vector))
			}
		}
	}
	// TODO: Refactor duplicate checks
	// ---------------------------
	for k, params := range s.VectorVamana {
		if v, ok := m[k]; ok {
			// This mess happens because we are dealing with arbitrary JSON.
			// Nothing stops the user from passing "vector": "memes" as valid
			// JSON. Furthermore, JSON by default decodes floats to float64.
			var vector []float32
			vector, ok := v.([]float32)
			if !ok {
				vectorAny, ok := v.([]interface{})
				if !ok {
					return fmt.Errorf("expected vector for property %s, got %T", k, v)
				}
				vector = make([]float32, len(vectorAny))
				for i, v := range vectorAny {
					switch v := v.(type) {
					case float32:
						vector[i] = v
					case float64:
						vector[i] = float32(v)
					default:
						return fmt.Errorf("expected float32 for %s, got %T", k, v)
					}
				}
			}
			if len(vector) != int(params.VectorSize) {
				return fmt.Errorf("expected vector of size %d for %s, got %d", params.VectorSize, k, len(vector))
			}
			// We override the map value with the vector so downstream code can
			// use the vector directly.
			m[k] = vector
		}
	}
	// ---------------------------
	for k := range s.Text {
		if v, ok := m[k]; ok {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected string for %s, got %T", k, v)
			}
		}
	}
	// ---------------------------
	for k := range s.String {
		if v, ok := m[k]; ok {
			if _, ok := v.(string); !ok {
				return fmt.Errorf("expected string for %s, got %T", k, v)
			}
		}
	}
	// ---------------------------
	for k := range s.Integer {
		if v, ok := m[k]; ok {
			if _, ok := v.(int); !ok {
				return fmt.Errorf("expected int for %s, got %T", k, v)
			}
		}
	}
	// ---------------------------
	for k := range s.Float {
		if v, ok := m[k]; ok {
			if _, ok := v.(float32); !ok {
				return fmt.Errorf("expected float for %s, got %T", k, v)
			}
		}
	}
	// ---------------------------
	for k := range s.StringArray {
		if v, ok := m[k]; ok {
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
	IndexVectorFlatParameters
	SearchSize  int     `json:"searchSize" binding:"min=25,max=75"`
	DegreeBound int     `json:"degreeBound" binding:"min=32,max=64"`
	Alpha       float32 `json:"alpha" binding:"min=1.1,max=1.5"`
}

type IndexTextParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}

type IndexStringParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}
