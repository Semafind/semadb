package models

type PlainQuantizerParameters struct{}

type BinaryQuantizerParamaters struct {
	Threshold        *float32 `json:"threshold"`
	TriggerThreshold int      `json:"triggerThreshold"`
}
