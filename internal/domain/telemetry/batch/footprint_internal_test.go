package batch

import "testing"

func TestMaxFootprintBoundsTheSpansAboveTheirWeight(t *testing.T) {
	type args struct {
		maxDecoded, maxSpans, minSpanWeight int64
	}
	tests := []struct {
		Name string
		Args args
		Want int64
	}{
		{
			Name: "spans lighter on the wire than in the journal add their difference",
			Args: args{maxDecoded: 1000, maxSpans: 10, minSpanWeight: 200},
			Want: 1560,
		},
		{
			Name: "spans heavier on the wire add nothing",
			Args: args{maxDecoded: 1000, maxSpans: 10, minSpanWeight: 512},
			Want: 1000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			if got := MaxFootprint(tt.Args.maxDecoded, tt.Args.maxSpans, tt.Args.minSpanWeight); got != tt.Want {
				t.Errorf("MaxFootprint(%+v) = %d, want %d", tt.Args, got, tt.Want)
			}
		})
	}
}
