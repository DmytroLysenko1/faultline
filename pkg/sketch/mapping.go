package sketch

import "math"

const (
	RelativeAccuracy = 0.02

	estimateScale = 2
)

type mapping struct {
	invLogGamma float64
	values      []int64
}

func newMapping() mapping {
	gamma := (1 + RelativeAccuracy) / (1 - RelativeAccuracy)
	invLogGamma := 1 / math.Log(gamma)
	top := binIndex(math.MaxInt64, invLogGamma)
	values := make([]int64, top+1)
	for i := range values {
		values[i] = estimate(gamma, i)
	}
	return mapping{
		invLogGamma: invLogGamma,
		values:      values,
	}
}

func (m mapping) index(nanos int64) uint16 {
	return binIndex(nanos, m.invLogGamma)
}

func (m mapping) value(index uint16) int64 {
	return m.values[index]
}

func binIndex(nanos int64, invLogGamma float64) uint16 {
	return uint16(math.Ceil(math.Log(float64(nanos)) * invLogGamma))
}

func estimate(gamma float64, index int) int64 {
	value := estimateScale * math.Pow(gamma, float64(index)) / (gamma + 1)
	if value >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(math.Round(value))
}
