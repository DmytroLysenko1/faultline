package decodelimit_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
)

type rejection struct {
	Err    error
	Weight int64
}

type rejections struct {
	Seen []rejection
}

func (r *rejections) record(err error, weight int64) {
	r.Seen = append(r.Seen, rejection{
		Err:    err,
		Weight: weight,
	})
}

type countingTarget struct {
	Calls int
}

func (c *countingTarget) UnmarshalProto([]byte) error {
	c.Calls++
	return nil
}

func newCodec(seen *rejections) *decodelimit.Codec {
	return decodelimit.NewCodec(production(), seen.record)
}

func buffers(wire []byte) mem.BufferSlice {
	return mem.BufferSlice{
		mem.SliceBuffer(wire),
	}
}

func TestCodecRejectsBeforeDecoding(t *testing.T) {
	var seen rejections
	target := &countingTarget{}
	err := newCodec(&seen).Unmarshal(buffers(hostileRequests()["empty spans"]), target)
	if !errors.Is(err, decodelimit.ErrTooHeavy) {
		t.Fatalf("err = %v, want %v", err, decodelimit.ErrTooHeavy)
	}
	if target.Calls != 0 {
		t.Errorf("UnmarshalProto called %d times for a rejected request, want 0", target.Calls)
	}
	if len(seen.Seen) != 1 || !errors.Is(seen.Seen[0].Err, decodelimit.ErrTooHeavy) || seen.Seen[0].Weight <= realLimit {
		t.Errorf("onReject saw %+v, want one ErrTooHeavy with the weight over the limit", seen.Seen)
	}
}

func TestCodecReportsBytesPdataRejectsAsMalformed(t *testing.T) {
	var seen rejections
	wrongWireType := message(1, varint(1))
	err := newCodec(&seen).Unmarshal(buffers(wrongWireType), new(ptraceotlp.NewExportRequest()))
	if !errors.Is(err, decodelimit.ErrMalformed) {
		t.Fatalf("err = %v, want %v", err, decodelimit.ErrMalformed)
	}
	if len(seen.Seen) != 1 || !errors.Is(seen.Seen[0].Err, decodelimit.ErrMalformed) {
		t.Errorf("onReject saw %+v, want one ErrMalformed", seen.Seen)
	}
}

func TestCodecDecodesLegitimateRequests(t *testing.T) {
	var seen rejections
	request := ptraceotlp.NewExportRequest()
	if err := newCodec(&seen).Unmarshal(buffers(demoRequest(t)), &request); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if request.Traces().SpanCount() == 0 {
		t.Errorf("decoded request has no spans")
	}
	if len(seen.Seen) != 0 {
		t.Errorf("onReject called for a legitimate request: %+v", seen.Seen)
	}
}

func TestCodecDelegatesMessagesItDoesNotOwn(t *testing.T) {
	var seen rejections
	codec := newCodec(&seen)
	original := durationpb.New(time.Second)
	encoded, err := codec.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	if diff := cmp.Diff(want, encoded.Materialize()); diff != "" {
		t.Errorf("delegated marshal (-want +got):\n%s", diff)
	}
	decoded := &durationpb.Duration{}
	if err := codec.Unmarshal(encoded, decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if diff := cmp.Diff(original.AsDuration(), decoded.AsDuration()); diff != "" {
		t.Errorf("delegated round trip (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff("proto", codec.Name()); diff != "" {
		t.Errorf("codec name (-want +got):\n%s", diff)
	}
}
