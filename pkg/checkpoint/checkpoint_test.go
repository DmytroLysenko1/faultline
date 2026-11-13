package checkpoint_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const (
	testVersion = 7
	first       = checkpoint.Tag(1)
	second      = checkpoint.Tag(2)
	headerBytes = 8
	frameBytes  = 5
)

type values struct {
	Uints     []uint64
	Ints      []int64
	Bools     []bool
	Strings   []string
	Times     []time.Time
	Durations []time.Duration
	Fixed     [][4]byte
}

func randomValues(rng *rand.Rand, count int) values {
	var v values
	for i := range count {
		v.Uints = append(v.Uints, rng.Uint64()>>rng.IntN(64))
		v.Ints = append(v.Ints, rng.Int64()>>rng.IntN(64)-rng.Int64N(math.MaxInt32))
		v.Bools = append(v.Bools, rng.IntN(2) == 0)
		v.Strings = append(v.Strings, strings.Repeat("ж", rng.IntN(checkpoint.MaxStringBytes/2+1)))
		v.Times = append(v.Times, time.Unix(rng.Int64N(1<<40)-1<<39, rng.Int64N(1e9)).UTC())
		v.Durations = append(v.Durations, time.Duration(rng.Int64()))
		v.Fixed = append(v.Fixed, [4]byte{byte(i), 1, 2, 3})
	}
	return v
}

func write(w *checkpoint.Writer, v *values) {
	for i := range v.Uints {
		w.Uint(v.Uints[i])
		w.Int(v.Ints[i])
		w.Bool(v.Bools[i])
		w.Text(v.Strings[i])
		w.Time(v.Times[i])
		w.Duration(v.Durations[i])
		w.Fixed(v.Fixed[i][:])
	}
}

func read(r *checkpoint.Reader, count int) values {
	var v values
	for range count {
		v.Uints = append(v.Uints, r.Uint())
		v.Ints = append(v.Ints, r.Int())
		v.Bools = append(v.Bools, r.Bool())
		v.Strings = append(v.Strings, r.Text(checkpoint.MaxStringBytes))
		v.Times = append(v.Times, r.Time())
		v.Durations = append(v.Durations, r.Duration())
		var fixed [4]byte
		r.Fixed(fixed[:])
		v.Fixed = append(v.Fixed, fixed)
	}
	return v
}

func encode(t *testing.T, sections ...values) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, testVersion)
	for i := range sections {
		w.Begin(checkpoint.Tag(i + 1))
		write(w, &sections[i])
		w.End()
	}
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func decode(data []byte, counts ...int) ([]values, error) {
	r := checkpoint.NewReader(bytes.NewReader(data))
	decoded := make([]values, 0, len(counts))
	for i, count := range counts {
		r.Begin(checkpoint.Tag(i + 1))
		decoded = append(decoded, read(r, count))
		r.End()
	}
	return decoded, r.Finish()
}

// TestValuesComeBackInOrderAcrossChunks: sections far larger than one chunk read back exactly,
// with the version, and an empty section is still a section.
func TestValuesComeBackInOrderAcrossChunks(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	tests := []struct {
		name   string
		counts []int
	}{
		{name: "one small section", counts: []int{3}},
		{name: "an empty section between two", counts: []int{5, 0, 5}},
		{name: "sections spanning many chunks", counts: []int{400, 900}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := make([]values, 0, len(tt.counts))
			for _, count := range tt.counts {
				want = append(want, randomValues(rng, count))
			}
			data := encode(t, want...)

			got, err := decode(data, tt.counts...)

			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("values mismatch (-want +got):\n%s", diff)
			}
			if version := checkpoint.NewReader(bytes.NewReader(data)).Version(); version != testVersion {
				t.Errorf("version = %d, want %d", version, testVersion)
			}
		})
	}
}

func TestTimesKeepTheirInstantAndTheZeroTime(t *testing.T) {
	want := []time.Time{
		{},
		time.Unix(0, 0).UTC(),
		time.Unix(-1, 999_999_999).UTC(),
		time.Unix(0, math.MaxInt64).UTC(),
		time.Unix(0, math.MinInt64).UTC(),
		time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC),
	}
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, testVersion)
	w.Begin(first)
	for _, at := range want {
		w.Time(at)
	}
	w.End()
	r := checkpoint.NewReader(&out)
	r.Begin(first)
	got := make([]time.Time, 0, len(want))
	for range want {
		got = append(got, r.Time())
	}
	r.End()

	if err := r.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("times mismatch (-want +got):\n%s", diff)
	}
	if !got[0].IsZero() {
		t.Errorf("the zero time came back as %v", got[0])
	}
}

func TestBoundedReadsRefuseWhatIsOverTheirLimit(t *testing.T) {
	tests := []struct {
		name string
		read func(r *checkpoint.Reader)
	}{
		{name: "count", read: func(r *checkpoint.Reader) { r.Count(9) }},
		{name: "index", read: func(r *checkpoint.Reader) { r.Index(10) }},
		{name: "index into nothing", read: func(r *checkpoint.Reader) { r.Index(0) }},
		{name: "int32", read: func(r *checkpoint.Reader) { r.Int32(9) }},
		{name: "uint8", read: func(r *checkpoint.Reader) { r.Uint8(9) }},
		{name: "uint16", read: func(r *checkpoint.Reader) { r.Uint16(9) }},
		{name: "uint32", read: func(r *checkpoint.Reader) { r.Uint32(9) }},
		{name: "bool", read: func(r *checkpoint.Reader) { r.Bool() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			w := checkpoint.NewWriter(&out, testVersion)
			w.Begin(first)
			w.Uint(10)
			w.End()
			r := checkpoint.NewReader(&out)
			r.Begin(first)

			tt.read(r)

			if !errors.Is(r.Err(), checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", r.Err())
			}
		})
	}
}

func sealed(t *testing.T) []byte {
	t.Helper()
	return encode(t, values{Uints: []uint64{1}, Ints: []int64{2}, Bools: []bool{true}, Strings: []string{"s"},
		Times: []time.Time{{}}, Durations: []time.Duration{3}, Fixed: [][4]byte{{}}})
}

// TestDamageIsCorruptionNotAnotherState: every way bytes can go wrong is ErrCorrupt.
func TestDamageIsCorruptionNotAnotherState(t *testing.T) {
	data := sealed(t)
	tests := []struct {
		name   string
		damage func([]byte) []byte
		counts []int
	}{
		{name: "nothing at all", damage: func([]byte) []byte { return nil }, counts: []int{1}},
		{name: "not a checkpoint", damage: func(d []byte) []byte { d[0] = 'X'; return d }, counts: []int{1}},
		{name: "cut inside a chunk", damage: func(d []byte) []byte { return d[:len(d)-6] }, counts: []int{1}},
		{name: "cut before the end chunk", damage: func(d []byte) []byte { return d[:len(d)-9] }, counts: []int{1}},
		{name: "a flipped payload byte", damage: func(d []byte) []byte { d[headerBytes+frameBytes] ^= 1; return d },
			counts: []int{1}},
		{name: "another section tag", damage: func(d []byte) []byte { d[headerBytes] = 9; return d }, counts: []int{1}},
		{name: "a chunk longer than allowed", damage: func(d []byte) []byte {
			binary.LittleEndian.PutUint32(d[headerBytes+1:], 1<<20)
			return d
		}, counts: []int{1}},
		{name: "trailing bytes", damage: func(d []byte) []byte { return append(d, 0) }, counts: []int{1}},
		{name: "values left unread", damage: func(d []byte) []byte { return d }, counts: []int{0}},
		{name: "reading past the section", damage: func(d []byte) []byte { return d }, counts: []int{2}},
		{name: "a missing section", damage: func(d []byte) []byte { return d }, counts: []int{1, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decode(tt.damage(bytes.Clone(data)), tt.counts...)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Errorf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

type failingWriter struct {
	after int
}

var errDisk = errors.New("disk full")

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.after <= 0 {
		return 0, errDisk
	}
	f.after--
	return len(p), nil
}

func TestTheWriterKeepsItsFirstError(t *testing.T) {
	tests := []struct {
		name    string
		out     *failingWriter
		write   func(w *checkpoint.Writer)
		wantErr error
	}{
		{name: "the disk fails", out: &failingWriter{after: 1}, write: func(w *checkpoint.Writer) {
			w.Begin(first)
			w.Uint(1)
			w.End()
		}, wantErr: errDisk},
		{name: "a value outside a section", out: &failingWriter{after: 9}, write: func(w *checkpoint.Writer) {
			w.Uint(1)
		}},
		{name: "a string over the limit", out: &failingWriter{after: 9}, write: func(w *checkpoint.Writer) {
			w.Begin(first)
			w.Text(strings.Repeat("a", checkpoint.MaxStringBytes+1))
		}},
		{name: "a negative count", out: &failingWriter{after: 9}, write: func(w *checkpoint.Writer) {
			w.Begin(first)
			w.Count(-1)
		}},
		{name: "a section inside a section", out: &failingWriter{after: 9}, write: func(w *checkpoint.Writer) {
			w.Begin(first)
			w.Begin(second)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := checkpoint.NewWriter(tt.out, testVersion)

			tt.write(w)

			if w.Err() == nil || tt.wantErr != nil && !errors.Is(w.Err(), tt.wantErr) {
				t.Errorf("err = %v, want %v", w.Err(), tt.wantErr)
			}
		})
	}
}

// FuzzReader: any bytes decode to an error or to values; the reader never panics and never
// allocates more than one chunk.
func FuzzReader(f *testing.F) {
	f.Add(sealed(&testing.T{}), 1)
	f.Fuzz(func(t *testing.T, data []byte, count int) {
		if count < 0 || count > 64 {
			return
		}
		r := checkpoint.NewReader(bytes.NewReader(data))
		r.Begin(first)
		for range count {
			r.Uint()
			r.Text(checkpoint.MaxStringBytes)
			r.Time()
			r.Count(1 << 20)
		}
		r.End()
		_ = r.Finish()
	})
}
