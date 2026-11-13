package clock

import "github.com/DmytroLysenko1/faultline/pkg/checkpoint"

func (c *Monotonic) WriteCheckpoint(w *checkpoint.Writer) {
	w.Bool(c.Started)
	w.Time(c.Epoch)
	w.Duration(c.Elapsed)
}

func Restore(r *checkpoint.Reader) Monotonic {
	restored := Monotonic{
		Started: r.Bool(),
		Epoch:   r.Time(),
		Elapsed: r.Duration(),
	}
	if restored.Elapsed < 0 || !restored.Started && restored != (Monotonic{}) {
		r.Fail("clock is not monotonic")
	}
	return restored
}
