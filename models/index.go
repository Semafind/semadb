package models

import (
	"fmt"
	"strings"
)

// Defines the index schema for a collection, each index type is a map of property names
// to index parameters. The index parameters are different for each index type.
type IndexSchema map[string]IndexSchemaValue

func (s IndexSchema) Validate() error {
	for _, v := range s {
		if err := v.Validate(); err != nil {
			return err
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

func (v IndexSchemaValue) Validate() error {
	if v.Type != IndexTypeVectorFlat &&
		v.Type != IndexTypeVectorVamana &&
		v.Type != IndexTypeText &&
		v.Type != IndexTypeString &&
		v.Type != IndexTypeInteger &&
		v.Type != IndexTypeFloat &&
		v.Type != IndexTypeStringArray {
		return fmt.Errorf("unknown index type %s", v.Type)
	}
	switch v.Type {
	case IndexTypeVectorFlat:
		if v.VectorFlat == nil {
			return fmt.Errorf("vectorFlat parameters not provided for type %s", v.Type)
		}
		return v.VectorFlat.Validate()
	case IndexTypeVectorVamana:
		if v.VectorVamana == nil {
			return fmt.Errorf("vectorVamana parameters not provided for type %s", v.Type)
		}
		return v.VectorVamana.Validate()
	case IndexTypeText:
		if v.Text == nil {
			return fmt.Errorf("text parameters not provided for type %s", v.Type)
		}
		return v.Text.Validate()
	case IndexTypeString:
		if v.String == nil {
			return fmt.Errorf("string parameters not provided for type %s", v.Type)
		}
		return v.String.Validate()
	case IndexTypeStringArray:
		if v.StringArray == nil {
			return fmt.Errorf("stringArray parameters not provided for type %s", v.Type)
		}
		return v.StringArray.Validate()
	case IndexTypeInteger:
		// Nothing to check
	case IndexTypeFloat:
	default:
		return fmt.Errorf("unknown index type %s", v.Type)
	}
	return nil
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
func (s IndexSchema) CheckCompatibleMap(pointMap PointAsMap) error {
	// We will go through each index field, check if the map has them, is of
	// right type and any extra checks needed
	// ---------------------------
	for property, schema := range s {
		// Let's the value of the property in the map.
		// Handle nested properties such as "nested.field"
		parts := strings.Split(property, ".")
		// m is the map, could be nested. k is the key of the property v is the
		// value of the property. For example, if property is "nested.field", m
		// is the map of "nested" and k is "field". It if is a flat property, m
		// is the map of the root and k is the property name.
		m := pointMap
		k := ""
		var v any
		// ---------------------------
		skip := false
		for i, part := range parts {
			partValue, ok := m[part]
			if !ok {
				// This property is not in the map
				skip = true
				break
			}
			if i == len(parts)-1 {
				v = partValue
				k = part
			} else {
				// Can we nest further?
				if nested, ok := partValue.(PointAsMap); ok {
					m = nested
					continue
				}
				// Despite defined as the same type, the above type assertion
				// does not cover this case. We assert again. Internally we use
				// PointAsMap but something like json decoding returns
				// map[string]any.
				if nested, ok := partValue.(map[string]any); ok {
					m = nested
					continue
				}
				return fmt.Errorf("expected nested map for property %s, got %T", part, partValue)
			}
		}
		// ---------------------------
		if skip {
			// This property is not in the map.
			continue
		}
		// ---------------------------
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

func (p IndexVectorFlatParameters) Validate() error {
	if p.VectorSize < 1 || p.VectorSize > 4096 {
		return fmt.Errorf("vector size must be between 1 and 4096, got %d", p.VectorSize)
	}
	if p.DistanceMetric != DistanceEuclidean &&
		p.DistanceMetric != DistanceCosine &&
		p.DistanceMetric != DistanceDot &&
		p.DistanceMetric != DistanceHamming &&
		p.DistanceMetric != DistanceJaccard &&
		p.DistanceMetric != DistanceHaversine {
		return fmt.Errorf("unknown distance metric %s", p.DistanceMetric)
	}
	if p.DistanceMetric == DistanceHaversine && p.VectorSize != 2 {
		return fmt.Errorf("haversine distance metric requires vector size 2 got %d", p.VectorSize)
	}
	if p.Quantizer != nil {
		return p.Quantizer.Validate()
	}
	return nil
}

type IndexVectorVamanaParameters struct {
	VectorSize     uint       `json:"vectorSize" binding:"required,min=1,max=4096"`
	DistanceMetric string     `json:"distanceMetric" binding:"required,oneof=euclidean cosine dot hamming jaccard haversine"`
	SearchSize     int        `json:"searchSize" binding:"min=25,max=75"`
	DegreeBound    int        `json:"degreeBound" binding:"min=32,max=64"`
	Alpha          float32    `json:"alpha" binding:"min=1.1,max=1.5"`
	Quantizer      *Quantizer `json:"quantizer,omitempty"`
}

func (p IndexVectorVamanaParameters) Validate() error {
	if p.VectorSize < 1 || p.VectorSize > 4096 {
		return fmt.Errorf("vector size must be between 1 and 4096, got %d", p.VectorSize)
	}
	if p.DistanceMetric != DistanceEuclidean &&
		p.DistanceMetric != DistanceCosine &&
		p.DistanceMetric != DistanceDot &&
		p.DistanceMetric != DistanceHamming &&
		p.DistanceMetric != DistanceJaccard &&
		p.DistanceMetric != DistanceHaversine {
		return fmt.Errorf("unknown distance metric %s", p.DistanceMetric)
	}
	if p.DistanceMetric == DistanceHaversine && p.VectorSize != 2 {
		return fmt.Errorf("haversine distance metric requires vector size 2 got %d", p.VectorSize)
	}
	if p.SearchSize < 25 || p.SearchSize > 75 {
		return fmt.Errorf("search size must be between 25 and 75, got %d", p.SearchSize)
	}
	if p.DegreeBound < 32 || p.DegreeBound > 64 {
		return fmt.Errorf("degree bound must be between 32 and 64, got %d", p.DegreeBound)
	}
	if p.Alpha < 1.1 || p.Alpha > 1.5 {
		return fmt.Errorf("alpha must be between 1.1 and 1.5, got %f", p.Alpha)
	}
	if p.Quantizer != nil {
		return p.Quantizer.Validate()
	}
	return nil

}

type IndexTextParameters struct {
	Analyser string `json:"analyser" binding:"required,oneof=standard"`
}

func (p IndexTextParameters) Validate() error {
	if p.Analyser != "standard" {
		return fmt.Errorf("unknown analyser %s", p.Analyser)
	}
	return nil
}

type IndexStringParameters struct {
	CaseSensitive bool `json:"caseSensitive"`
}

func (p IndexStringParameters) Validate() error {
	return nil
}

type IndexStringArrayParameters struct {
	IndexStringParameters
}

func (p IndexStringArrayParameters) Validate() error {
	return p.IndexStringParameters.Validate()
}
