package models

/* The search query design is based on the following key steps:
 *
 * 1. Filter first to narrow down search space.
 * 2. Then vector or text search and combine with hybrid weights.
 * 3. Maybe select or sort the data other than score with added vector _distance and _score.
 * 4. Then offset and limit the data, something like pagination.
 */

// ---------------------------

type SearchQuery struct {
	Filters       []SearchFilter     `json:"filter" binding:"dive"`
	Search        []SearchProperty   `json:"search" binding:"dive"`
	HybridWeights map[string]float32 `json:"hybridWeights"`
	Select        []string           `json:"select"`
	Sort          []string           `json:"sort"`
	Offset        int                `json:"offset" binding:"min=0"`
	Limit         int                `json:"limit" binding:"required,min=1,max=100"`
}

// ---------------------------

type SearchProperty struct {
	Property     string                     `json:"property" binding:"required"`
	VectorFlat   *SearchVectorFlatOptions   `json:"vectorFlat" binding:"dive"`
	VectorVamana *SearchVectorVamanaOptions `json:"vectorVamana" binding:"dive"`
	Text         *SearchTextOptions         `json:"text" binding:"dive"`
	// We can't do search on string, integer, float, stringArray because they
	// don't provide an ordering, they are used instead as filters.
}

// ---------------------------

/* The filter is explicit in its typing so any incoming query is validated easily
 * without any custom parsing and validation on interface{} types. */
type SearchFilter struct {
	Property     string                     `json:"property" binding:"required"`
	VectorFlat   *SearchVectorFlatOptions   `json:"vectorFlat" binding:"dive"`
	VectorVamana *SearchVectorVamanaOptions `json:"vectorVamana" binding:"dive"`
	Text         *SearchTextOptions         `json:"text" binding:"dive"`
	String       *SearchStringOptions       `json:"string" binding:"dive"`
	Integer      *SearchIntegerOptions      `json:"integer" binding:"dive"`
	Float        *SearchFloatOptions        `json:"float" binding:"dive"`
	StringArray  *SearchStringArrayOptions  `json:"stringArray" binding:"dive"`
	Not          *SearchFilter              `json:"_not" binding:"dive"`
	And          *SearchFilter              `json:"_and" binding:"dive"`
	Or           *SearchFilter              `json:"_or" binding:"dive"`
}

// ---------------------------

type SearchVectorVamanaOptions struct {
	Vector     []float32 `json:"vector" binding:"required,max=4096"`
	Operator   string    `json:"operator" binding:"required,oneof=near"`
	Limit      int       `json:"limit" binding:"required,min=1,max=75"`
	SearchSize int       `json:"searchSize" binding:"required,min=25,max=75"`
}

type SearchVectorFlatOptions struct {
	Vector   []float32 `json:"vector" binding:"required,max=4096"`
	Operator string    `json:"operator" binding:"required,oneof=near"`
	Limit    int       `json:"limit" binding:"required,min=1,max=75"`
}

type SearchTextOptions struct {
	Value    string `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=containsAll containsAny"`
	Limit    int    `json:"limit" binding:"required,min=1,max=75"`
}

type SearchStringOptions struct {
	Value    string `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=equals notEquals startsWith greaterThan greaterThanOrEquals lessThan lessThanOrEquals"`
}

type SearchIntegerOptions struct {
	Value    int    `json:"value" binding:"required"`
	Operator string `json:"operator" binding:"required,oneof=equals notEquals greaterThan greaterThanOrEquals lessThan lessThanOrEquals"`
}

type SearchFloatOptions struct {
	Value    float32 `json:"value" binding:"required"`
	Operator string  `json:"operator" binding:"required,oneof=equals notEquals greaterThan greaterThanOrEquals lessThan lessThanOrEquals"`
}

type SearchStringArrayOptions struct {
	Value    []string `json:"value" binding:"required"`
	Operator string   `json:"operator" binding:"required,oneof=containsAll containsAny"`
}
