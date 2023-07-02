package models

type Point struct {
	Id       string
	Vector   []float32
	Version  int64
	Metadata []byte
}
