package models

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
