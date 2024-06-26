package models

type UserPlan struct {
	Name string `yaml:"name"`
	// Maximum number of collections
	MaxCollections int `yaml:"maxCollections"`
	// Maximum number of points in a collection
	MaxCollectionPointCount int64 `yaml:"maxCollectionPointCount"`
	// Maximum size of a point in a collection after encoding
	MaxPointSize int `yaml:"maxPointSize"`
	// The minimum amount of time in seconds between shard backups
	ShardBackupFrequency int `yaml:"shardBackupFrequency"`
	// The maximum number of shard backups to keep
	ShardBackupCount int `yaml:"shardBackupCount"`
}
