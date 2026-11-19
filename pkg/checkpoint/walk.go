package checkpoint

import "io"

func Walk(in io.Reader, sections ...Tag) error {
	r := NewReader(in)
	for _, tag := range sections {
		r.Begin(tag)
		r.skip()
		r.End()
	}
	return r.Finish()
}

func (r *Reader) skip() {
	for r.err == nil && !r.ended {
		r.payload = r.payload[:0]
		r.next()
	}
}
