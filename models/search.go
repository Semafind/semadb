package models

import (
	"fmt"

	"github.com/google/uuid"
)

/* The search query design is based on the following key steps:
 *
 * 1. Filter first to narrow down search space.
 * 2. Then vector or text search and combine with hybrid weights.
 * 3. Maybe select or sort the data other than score with added vector _distance and _score.
 * 4. Then offset and limit the data, something like pagination.
 */

// ---------------------------

type SearchRequest struct {
	Query  Query        `json:"query" binding:"required"`
	Select []string     `json:"select"`
	Sort   []SortOption `json:"sort" binding:"max=10,dive"`
	Offset int          `json:"offset" binding:"min=0"`
	Limit  int          `json:"limit" binding:"required,min=1,max=100"`
}

func (r SearchRequest) Validate() error {
	// Query validation happens when we know the schema. At this point we can only validate other fields.
	if err := r.Query.Validate(); err != nil {
		return fmt.Errorf("query validation failed: %v", err)
	}
	// ---------------------------
	if len(r.Sort) > 10 {
		return fmt.Errorf("sort options exceed maximum of 10")
	}
	for _, sort := range r.Sort {
		if err := sort.Validate(); err != nil {
			return fmt.Errorf("sort validation failed: %v", err)
		}
	}
	// ---------------------------
	if r.Offset < 0 {
		return fmt.Errorf("offset must be greater than or equal to 0")
	}
	if r.Limit < 1 || r.Limit > 100 {
		return fmt.Errorf("limit must be between 1 and 100")
	}
	// ---------------------------
	return nil
}

// ---------------------------

type Query struct {
	Property     string                     `json:"property" binding:"required"`
	VectorFlat   *SearchVectorFlatOptions   `json:"vectorFlat"`
	VectorVamana *SearchVectorVamanaOptions `json:"vectorVamana"`
	Text         *SearchTextOptions         `json:"text"`
	String       *SearchStringOptions       `json:"string"`
	Integer      *SearchIntegerOptions      `json:"integer"`
	Float        *SearchFloatOptions        `json:"float"`
	StringArray  *SearchStringArrayOptions  `json:"stringArray"`
	And          []Query                    `json:"_and" binding:"dive"`
	Or           []Query                    `json:"_or" binding:"dive"`
}

func (q Query) Validate() error {
	if len(q.Property) == 0 {
		return fmt.Errorf("query property cannot be empty")
	}
	// ---------------------------
	if q.VectorFlat != nil {
		if err := q.VectorFlat.Validate(); err != nil {
			return fmt.Errorf("vectorFlat validation failed: %v", err)
		}
	}
	if q.VectorVamana != nil {
		if err := q.VectorVamana.Validate(); err != nil {
			return fmt.Errorf("vectorVamana validation failed: %v", err)
		}
	}
	if q.Text != nil {
		if err := q.Text.Validate(); err != nil {
			return fmt.Errorf("text validation failed: %v", err)
		}
	}
	if q.String != nil {
		if err := q.String.Validate(); err != nil {
			return fmt.Errorf("string validation failed: %v", err)
		}
	}
	if q.Integer != nil {
		if err := q.Integer.Validate(); err != nil {
			return fmt.Errorf("integer validation failed: %v", err)
		}
	}
	if q.Float != nil {
		if err := q.Float.Validate(); err != nil {
			return fmt.Errorf("float validation failed: %v", err)
		}
	}
	if q.StringArray != nil {
		if err := q.StringArray.Validate(); err != nil {
			return fmt.Errorf("stringArray validation failed: %v", err)
		}
	}
	// ---------------------------
	if q.Property == "_and" && len(q.And) == 0 {
		return fmt.Errorf("and query must have at least one subquery")
	}
	if q.Property == "_or" && len(q.Or) == 0 {
		return fmt.Errorf("or query must have at least one subquery")
	}
	if len(q.And) > 0 {
		for i, subQuery := range q.And {
			if err := subQuery.Validate(); err != nil {
				return fmt.Errorf("and[%d] validation failed: %v", i, err)
			}
		}
	}
	if len(q.Or) > 0 {
		for i, subQuery := range q.Or {
			if err := subQuery.Validate(); err != nil {
				return fmt.Errorf("or[%d] validation failed: %v", i, err)
			}
		}
	}
	// ---------------------------
	if q.Property == "_id" {
		// Either string with Equals operator or stringArray with ContainsAny operator
		switch {
		case q.String != nil:
			if q.String.Operator != OperatorEquals {
				return fmt.Errorf("invalid operator %s for %s, expected %s", q.String.Operator, q.Property, OperatorEquals)
			}
			if _, err := uuid.Parse(q.String.Value); err != nil {
				return fmt.Errorf("invalid UUID %v for %s, %v", q.String.Value, q.Property, err)
			}
		case q.StringArray != nil:
			if q.StringArray.Operator != OperatorContainsAny {
				return fmt.Errorf("invalid operator %s for %s, expected %s", q.StringArray.Operator, q.Property, OperatorContainsAny)
			}
			for _, v := range q.StringArray.Value {
				if _, err := uuid.Parse(v); err != nil {
					return fmt.Errorf("invalid UUID %s for %s, %v", v, q.Property, err)
				}
			}
		default:
			return fmt.Errorf("invalid query for _id, expected string or stringArray")
		}
	}
	return nil
}

func (q Query) ValidateSchema(schema IndexSchema) error {
	// Handle recursive case
	switch q.Property {
	case "_and":
		for _, subQuery := range q.And {
			if err := subQuery.ValidateSchema(schema); err != nil {
				return err
			}
		}
		return nil
	case "_or":
		for _, subQuery := range q.Or {
			if err := subQuery.ValidateSchema(schema); err != nil {
				return err
			}
		}
		return nil
	case "_id":
		return nil
	}
	// Handle base case
	value, ok := schema[q.Property]
	if !ok {
		return fmt.Errorf("property %s not found in index schema, cannot query", q.Property)
	}
	// Are the options given correctly?
	switch value.Type {
	case IndexTypeVectorFlat:
		if q.VectorFlat == nil {
			return fmt.Errorf("vectorFlat query options not provided for property %s", q.Property)
		}
		if len(q.VectorFlat.Vector) != int(value.VectorFlat.VectorSize) {
			return fmt.Errorf("vectorFlat query vector length mismatch for property %s, expected %d got %d", q.Property, value.VectorFlat.VectorSize, len(q.VectorFlat.Vector))
		}
		if q.VectorFlat.Filter != nil {
			if err := q.VectorFlat.Filter.ValidateSchema(schema); err != nil {
				return err
			}
		}
	case IndexTypeVectorVamana:
		if q.VectorVamana == nil {
			return fmt.Errorf("vectorVamana query options not provided for property %s", q.Property)
		}
		if len(q.VectorVamana.Vector) != int(value.VectorVamana.VectorSize) {
			return fmt.Errorf("vectorVamana query vector length mismatch for property %s, expected %d got %d", q.Property, value.VectorVamana.VectorSize, len(q.VectorVamana.Vector))
		}
		if q.VectorVamana.Filter != nil {
			if err := q.VectorVamana.Filter.ValidateSchema(schema); err != nil {
				return err
			}
		}
	case IndexTypeText:
		if q.Text == nil {
			return fmt.Errorf("text query options not provided for property %s", q.Property)
		}
		if q.Text.Filter != nil {
			if err := q.Text.Filter.ValidateSchema(schema); err != nil {
				return err
			}
		}
	case IndexTypeString:
		if q.String == nil {
			return fmt.Errorf("string query options not provided for property %s", q.Property)
		}
	case IndexTypeStringArray:
		if q.StringArray == nil {
			return fmt.Errorf("stringArray query options not provided for property %s", q.Property)
		}
	case IndexTypeInteger:
		if q.Integer == nil {
			return fmt.Errorf("integer query options not provided for property %s", q.Property)
		}
	case IndexTypeFloat:
		if q.Float == nil {
			return fmt.Errorf("float query options not provided for property %s", q.Property)
		}
	default:
		return fmt.Errorf("unknown index type %s", value.Type)
	}
	return nil
}

// Shared search result struct for ordered search results
type SearchResult struct {
	Point
	// Used to transmit partially decoded data to the client
	DecodedData PointAsMap
	// Internal NodeId is not exposed to the client
	NodeId uint64 `json:"-" msgpack:"-"`
	// Pointers are used to differentiate between zero values and unset values.
	// A distance or score of 0 could be valid.  Computed from vector indices,
	// lower is better
	Distance *float32 `json:"_distance,omitempty" msgpack:"_distance,omitempty"`
	// Computed from generic indices, higher is better
	Score *float32 `json:"_score,omitempty" msgpack:"_score,omitempty"`
	// Combined final score
	HybridScore float32 `json:"_hybridScore" msgpack:"_hybridScore"`
}

// ---------------------------

type SortOption struct {
	Property   string `json:"property" binding:"required"`
	Descending bool   `json:"descending"`
}

func (s SortOption) Validate() error {
	if len(s.Property) == 0 {
		return fmt.Errorf("sorting property cannot be empty")
	}
	return nil
}

type SearchVectorVamanaOptions struct {
	Vector     []float32 `json:"vector" binding:"required,max=4096"`
	Operator   string    `json:"operator" binding:"required,oneof=near"`
	SearchSize int       `json:"searchSize" binding:"required,min=25,max=75"`
	Limit      int       `json:"limit" binding:"required,min=1,max=75"`
	Filter     *Query    `json:"filter"`
	Weight     *float32  `json:"weight"`
}

func (o SearchVectorVamanaOptions) Validate() error {
	// ---------------------------
	if len(o.Vector) < 1 || len(o.Vector) > 4096 {
		return fmt.Errorf("query vector length must be between 1 and 4096, got %d", len(o.Vector))
	}
	// ---------------------------
	if o.Operator != OperatorNear {
		return fmt.Errorf("invalid operator %s for vector query, expected %s", o.Operator, OperatorNear)
	}
	// ---------------------------
	if o.SearchSize < 25 || o.SearchSize > 75 {
		return fmt.Errorf("invalid searchSize %d for vector query, expected 25-75", o.SearchSize)
	}
	// ---------------------------
	if o.Limit < 1 || o.Limit > 75 {
		return fmt.Errorf("invalid limit %d for vector query, expected 1-75", o.Limit)
	}
	// ---------------------------
	if o.SearchSize < o.Limit {
		return fmt.Errorf("searchSize must be greater than or equal to limit")
	}
	// ---------------------------
	if o.Filter != nil {
		if err := o.Filter.Validate(); err != nil {
			return fmt.Errorf("filter validation failed: %v", err)
		}
	}
	// ---------------------------
	return nil
}

type SearchVectorFlatOptions struct {
	Vector   []float32 `json:"vector" binding:"required,max=4096"`
	Operator string    `json:"operator" binding:"required,oneof=near"`
	Limit    int       `json:"limit" binding:"required,min=1,max=75"`
	Filter   *Query    `json:"filter"`
	Weight   *float32  `json:"weight"`
}

func (o SearchVectorFlatOptions) Validate() error {
	// ---------------------------
	if len(o.Vector) < 1 || len(o.Vector) > 4096 {
		return fmt.Errorf("query vector length must be between 1 and 4096, got %d", len(o.Vector))
	}
	// ---------------------------
	if o.Operator != OperatorNear {
		return fmt.Errorf("invalid operator %s for vector query, expected %s", o.Operator, OperatorNear)
	}
	// ---------------------------
	if o.Limit < 1 || o.Limit > 75 {
		return fmt.Errorf("invalid limit %d for vector query, expected 1-75", o.Limit)
	}
	// ---------------------------
	if o.Filter != nil {
		if err := o.Filter.Validate(); err != nil {
			return fmt.Errorf("filter validation failed: %v", err)
		}
	}
	// ---------------------------
	return nil
}

type SearchTextOptions struct {
	Value    string   `json:"value" binding:"required"`
	Operator string   `json:"operator" binding:"required,oneof=containsAll containsAny"`
	Limit    int      `json:"limit" binding:"required,min=1,max=75"`
	Filter   *Query   `json:"filter"`
	Weight   *float32 `json:"weight"`
}

func (o SearchTextOptions) Validate() error {
	// ---------------------------
	if len(o.Value) == 0 {
		return fmt.Errorf("text query value cannot be empty")
	}
	// ---------------------------
	switch o.Operator {
	case OperatorContainsAll:
	case OperatorContainsAny:
	default:
		return fmt.Errorf("invalid operator %s for text query, expected %s or %s", o.Operator, OperatorContainsAll, OperatorContainsAny)
	}
	// ---------------------------
	if o.Limit < 1 || o.Limit > 75 {
		return fmt.Errorf("invalid limit %d for text query, expected 1-75", o.Limit)
	}
	// ---------------------------
	if o.Filter != nil {
		if err := o.Filter.Validate(); err != nil {
			return fmt.Errorf("filter validation failed: %v", err)
		}
	}
	// ---------------------------
	return nil
}

type SearchStringOptions struct {
	Value    string `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=equals notEquals startsWith greaterThan greaterThanOrEquals lessThan lessThanOrEquals inRange"`
	// Used for range queries
	EndValue string `json:"endValue"`
}

func (o SearchStringOptions) Validate() error {
	if len(o.Value) == 0 {
		return fmt.Errorf("string query value cannot be empty")
	}
	switch o.Operator {
	case OperatorEquals, OperatorNotEquals, OperatorStartsWith:
	case OperatorGreaterThan, OperatorGreaterOrEq:
	case OperatorLessThan, OperatorLessOrEq:
	case OperatorInRange:
		if o.EndValue <= o.Value {
			return fmt.Errorf("endValue must be greater than value for string range query")
		}
	default:
		return fmt.Errorf("invalid operator %s for string query", o.Operator)
	}
	return nil
}

type SearchIntegerOptions struct {
	Value    int64  `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=equals notEquals greaterThan greaterThanOrEquals lessThan lessThanOrEquals inRange"`
	EndValue int64  `json:"endValue"`
}

func (o SearchIntegerOptions) Validate() error {
	switch o.Operator {
	case OperatorEquals, OperatorNotEquals:
	case OperatorGreaterThan, OperatorGreaterOrEq:
	case OperatorLessThan, OperatorLessOrEq:
	case OperatorInRange:
		if o.EndValue <= o.Value {
			return fmt.Errorf("endValue must be greater than value for integer range query")
		}
	default:
		return fmt.Errorf("invalid operator %s for integer query", o.Operator)
	}
	return nil
}

type SearchFloatOptions struct {
	Value    float64 `json:"value" binding:"required"`
	Operator string  `json:"operator" binding:"required,oneof=equals notEquals greaterThan greaterThanOrEquals lessThan lessThanOrEquals inRange"`
	EndValue float64 `json:"endValue"`
}

func (o SearchFloatOptions) Validate() error {
	switch o.Operator {
	case OperatorEquals, OperatorNotEquals:
	case OperatorGreaterThan, OperatorGreaterOrEq:
	case OperatorLessThan, OperatorLessOrEq:
	case OperatorInRange:
		if o.EndValue <= o.Value {
			return fmt.Errorf("endValue must be greater than value for float range query")
		}
	default:
		return fmt.Errorf("invalid operator %s for float query", o.Operator)
	}
	return nil
}

type SearchStringArrayOptions struct {
	Value    []string `json:"value" binding:"required"`
	Operator string   `json:"operator" binding:"required,oneof=containsAll containsAny"`
}

func (o SearchStringArrayOptions) Validate() error {
	if len(o.Value) == 0 {
		return fmt.Errorf("stringArray query value cannot be empty")
	}
	switch o.Operator {
	case OperatorContainsAll:
	case OperatorContainsAny:
	default:
		return fmt.Errorf("invalid operator %s for stringArray query, expected %s or %s", o.Operator, OperatorContainsAll, OperatorContainsAny)
	}
	return nil
}
