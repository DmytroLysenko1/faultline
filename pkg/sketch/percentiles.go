package sketch

const (
	percentileCount = 3

	quantile50 = 0.50
	quantile95 = 0.95
	quantile99 = 0.99
)

func (a *Arena) Percentiles(s *Sketch) (Percentiles, bool) {
	a.generations.check(s)
	if s.count == 0 {
		return Percentiles{}, false
	}
	ranks := [...]uint64{
		rankOf(s.count, quantile50),
		rankOf(s.count, quantile95),
		rankOf(s.count, quantile99),
	}
	values := a.valueAtRanks(s, ranks)
	return Percentiles{
		P50: values[0],
		P95: values[1],
		P99: values[2],
	}, true
}

func rankOf(count uint64, quantile float64) uint64 {
	return uint64(quantile * float64(count-1))
}

func (a *Arena) valueAtRanks(s *Sketch, ranks [percentileCount]uint64) [percentileCount]int64 {
	var values [percentileCount]int64
	cumulative := uint64(s.zero)
	next := fillRanks(&values, ranks, 0, cumulative, 0)
	for _, e := range a.entries(s) {
		cumulative += e.count()
		next = fillRanks(&values, ranks, next, cumulative, a.mapping.value(e.index()))
	}
	return values
}

func fillRanks(
	values *[percentileCount]int64,
	ranks [percentileCount]uint64,
	next int,
	cumulative uint64,
	value int64,
) int {
	for next < percentileCount && ranks[next] < cumulative {
		values[next] = value
		next++
	}
	return next
}
