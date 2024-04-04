package models

/* The search query design is based on the following key steps:
 *
 * 1. Filter first to narrow down search space.
 * 2. Then vector or text search and combine with hybrid weights.
 * 3. Then offset and limit the data, something like pagination.
 * 4. Maybe select or sort the data other than score with added vector _distance and _score.
 */

// ---------------------------

type SearchRequest struct {
	Query  Query        `json:"query" binding:"required"`
	Select []string     `json:"select"`
	Sort   []SortOption `json:"sort"`
	Offset int          `json:"offset" binding:"min=0"`
	Limit  int          `json:"limit" binding:"required,min=1,max=100"`
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

// Shared search result struct for ordered search results
type SearchResult struct {
	Point
	NodeId uint64 `json:"-" msgpack:"-"` // NodeId is not exposed to the client
	/* Pointers are used to differentiate between zero values and unset values. A
	 * distance or score of 0 could be valid. */
	// Computed from vector indices, lower is better
	Distance *float32 `json:"_distance,omitempty" msgpack:"_distance,omitempty"`
	// Computed from generic indices, higher is better
	Score *float32 `json:"_score,omitempty" msgpack:"_score,omitempty"`
	// Combined final score
	FinalScore *float32 `json:"_finalScore,omitempty" msgpack:"_finalScore,omitempty"`
}

// ---------------------------

type SortOption struct {
	Property   string `json:"property" binding:"required"`
	Descending bool   `json:"descending"`
}

type SearchVectorVamanaOptions struct {
	Vector   []float32 `json:"vector" binding:"required,max=4096"`
	Operator string    `json:"operator" binding:"required,oneof=near"`
	Limit    int       `json:"limit" binding:"required,min=1,max=75"`
	Filter   *Query    `json:"filter"`
	Weight   float32   `json:"weight"`
}

type SearchVectorFlatOptions struct {
	Vector   []float32 `json:"vector" binding:"required,max=4096"`
	Operator string    `json:"operator" binding:"required,oneof=near"`
	Limit    int       `json:"limit" binding:"required,min=1,max=75"`
	Filter   *Query    `json:"filter"`
	Weight   float32   `json:"weight"`
}

type SearchTextOptions struct {
	Value    string   `json:"value" binding:"required"`
	Operator string   `json:"operator" binding:"required,oneof=containsAll containsAny"`
	Limit    int      `json:"limit" binding:"required,min=1,max=75"`
	Filter   *Query   `json:"filter"`
	Weight   *float32 `json:"weight"`
}

type SearchStringOptions struct {
	Value    string `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=equals notEquals startsWith greaterThan greaterThanOrEquals lessThan lessThanOrEquals inRange"`
	// Used for range queries
	EndValue string `json:"endValue"`
}

type SearchIntegerOptions struct {
	Value    int    `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=equals notEquals greaterThan greaterThanOrEquals lessThan lessThanOrEquals inRange"`
	EndValue string `json:"endValue"`
}

type SearchFloatOptions struct {
	Value    float32 `json:"value" binding:"required"`
	Operator string  `json:"operator" binding:"required,oneof=equals notEquals greaterThan greaterThanOrEquals lessThan lessThanOrEquals inRange"`
	EndValue string  `json:"endValue"`
}

type SearchStringArrayOptions struct {
	Value    []string `json:"value" binding:"required"`
	Operator string   `json:"operator" binding:"required,oneof=containsAll containsAny"`
}
