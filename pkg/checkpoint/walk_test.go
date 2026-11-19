package checkpoint_test

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/DmytroLysenko1/faultline/pkg/checkpoint"
)

const walkValues = 20_000

func walkImage(t testing.TB) []byte {
	t.Helper()
	var out bytes.Buffer
	w := checkpoint.NewWriter(&out, testVersion)
	rng := rand.New(rand.NewPCG(1, 2))
	for _, tag := range []checkpoint.Tag{first, second} {
		w.Begin(tag)
		for range walkValues {
			w.Uint(rng.Uint64())
			w.Text("value")
		}
		w.End()
	}
	w.Begin(checkpoint.Tag(3))
	w.End()
	if err := w.Err(); err != nil {
		t.Fatalf("write: %v", err)
	}
	return out.Bytes()
}

func TestWalkPassesAnImageWithoutDecodingItsValues(t *testing.T) {
	image := walkImage(t)

	err := checkpoint.Walk(bytes.NewReader(image), first, second, checkpoint.Tag(3))

	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
}

func TestWalkFindsDamageTheDigestCannotSee(t *testing.T) {
	image := walkImage(t)
	tests := []struct {
		name     string
		image    func() []byte
		sections []checkpoint.Tag
	}{
		{name: "a flipped bit", image: func() []byte {
			damaged := bytes.Clone(image)
			damaged[len(damaged)/2] ^= 1
			return damaged
		}, sections: []checkpoint.Tag{first, second, 3}},
		{name: "a truncated spool", image: func() []byte { return image[:len(image)-frameBytes] },
			sections: []checkpoint.Tag{first, second, 3}},
		{name: "trailing bytes", image: func() []byte { return append(bytes.Clone(image), 0) },
			sections: []checkpoint.Tag{first, second, 3}},
		{name: "sections in another order", image: func() []byte { return image },
			sections: []checkpoint.Tag{second, first, 3}},
		{name: "a missing section", image: func() []byte { return image },
			sections: []checkpoint.Tag{first, second, 3, 4}},
		{name: "not a checkpoint", image: func() []byte { return []byte("spool") }, sections: []checkpoint.Tag{first}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkpoint.Walk(bytes.NewReader(tt.image()), tt.sections...)

			if !errors.Is(err, checkpoint.ErrCorrupt) {
				t.Fatalf("Walk: err = %v, want %v", err, checkpoint.ErrCorrupt)
			}
		})
	}
}

func FuzzWalk(f *testing.F) {
	f.Add(walkImage(f))
	f.Fuzz(func(t *testing.T, image []byte) {
		err := checkpoint.Walk(bytes.NewReader(image), first, second, checkpoint.Tag(3))

		if err != nil && !errors.Is(err, checkpoint.ErrCorrupt) {
			t.Fatalf("Walk: err = %v, want nil or %v", err, checkpoint.ErrCorrupt)
		}
	})
}

func BenchmarkWalk(b *testing.B) {
	image := walkImage(b)
	b.SetBytes(int64(len(image)))
	for b.Loop() {
		if err := checkpoint.Walk(bytes.NewReader(image), first, second, checkpoint.Tag(3)); err != nil {
			b.Fatalf("Walk: %v", err)
		}
	}
}
