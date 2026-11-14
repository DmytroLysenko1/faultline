package atomicmax

import "cmp"

type cell[T cmp.Ordered] interface {
	Load() T
	CompareAndSwap(old, updated T) bool
}

func Raise[T cmp.Ordered](target cell[T], value T) {
	for {
		seen := target.Load()
		if value <= seen || target.CompareAndSwap(seen, value) {
			return
		}
	}
}
