//go:build !faultline_debug

package sketch

type generations struct{}

func newGenerations(int) generations {
	return generations{}
}

func (generations) current(uint32) uint32 {
	return 0
}

func (generations) retire(uint32) {}

func (generations) check(*Sketch) {}

func (generations) touch(*Sketch) {}

func generationsBytes(int) int64 {
	return 0
}
