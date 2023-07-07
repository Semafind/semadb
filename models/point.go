package models

import "github.com/google/uuid"

type Point struct {
	Id        uuid.UUID
	Vector    []float32
	Timestamp int64
	Metadata  []byte
}
