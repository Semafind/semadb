package distance

type DistFunc func(x, y []float32) float32

var EuclideanDistance = eucDistPureGo
var CosineDistance = cosineDistPureGo

func eucDistPureGo(x, y []float32) float32 {
	var sum float32
	for i := range x {
		diff := x[i] - y[i]
		sum += diff * diff
	}
	return sum
}

func dotProductDistPureGo(x, y []float32) float32 {
	var sum float32
	for i := range x {
		sum += x[i] * y[i]
	}
	return -sum
}

func cosineDistPureGo(x, y []float32) float32 {
	return 1 + dotProductDistPureGo(x, y)
}
