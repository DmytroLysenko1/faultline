package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

func TestOptionsRefuseAStoreThatCouldNotKeepItsPromises(t *testing.T) {
	tests := []struct {
		name    string
		change  func(o *Options)
		wantErr error
	}{
		{name: "valid", change: func(*Options) {}},
		{name: "partition outside the topology", change: func(o *Options) { o.Partition = testPartitions },
			wantErr: ErrInvalidOptions},
		{name: "no epoch", change: func(o *Options) { o.Epoch = 0 }, wantErr: ErrInvalidOptions},
		{name: "no lease ttl", change: func(o *Options) { o.LeaseTTL = 0 }, wantErr: ErrInvalidOptions},
		{name: "no upload rate", change: func(o *Options) { o.UploadRate = 0 }, wantErr: ErrInvalidOptions},
		{name: "negative verify rate", change: func(o *Options) { o.VerifyRate = -1 }, wantErr: ErrInvalidOptions},
		{name: "no worst image", change: func(o *Options) { o.WorstImage = 0 }, wantErr: ErrInvalidOptions},
		{
			name:    "bucket under 4.4 worst images",
			change:  func(o *Options) { o.MaxBytes = bucketFor(o.WorstImage) - 1 },
			wantErr: ErrInvalidOptions,
		},
		{name: "bucket of exactly 4.4 worst images", change: func(o *Options) { o.MaxBytes = bucketFor(o.WorstImage) }},
		{
			name: "a worst cycle that outlives the dedup window",
			change: func(o *Options) {
				o.UploadRate = 1 << 20
				o.WorstImage = 110 << 20
				o.MaxBytes = 1 << 30
			},
			wantErr: ErrInvalidOptions,
		},
		{
			name: "a slow verify rate makes the cycle outlive the window",
			change: func(o *Options) {
				o.VerifyRate = 64 << 10
				o.MaxBytes = 1 << 30
			},
			wantErr: ErrInvalidOptions,
		},
		{name: "invalid topology", change: func(o *Options) { o.Topology.Partitions = 0 }, wantErr: ErrInvalidOptions},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := testOptions()
			tt.change(options)

			err := options.Validate()

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("validate: err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNoOptionsAreInvalid(t *testing.T) {
	var options *Options

	if err := options.Validate(); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("validate(nil): err = %v, want %v", err, ErrInvalidOptions)
	}
}

func TestTheOrphanAgeOutlivesTheSlowestHolderUpload(t *testing.T) {
	options := testOptions()

	got := options.orphanAge()

	upload := time.Duration(float64(options.WorstImage) / float64(options.UploadRate) * float64(time.Second))
	if want := upload + uploadSlack + options.LeaseTTL + clockMargin; got != want {
		t.Fatalf("orphanAge = %s, want %s", got, want)
	}
}

func TestAPointerIsTrustedOnlyWhenEveryFieldMakesSense(t *testing.T) {
	good := objectRef{Name: "7-42", NUID: "N", Digest: digestPrefix + "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU=",
		Size: 9, Position: 42}
	tests := []struct {
		name    string
		data    string
		change  func(ref *objectRef)
		wantErr error
	}{
		{name: "valid", change: func(*objectRef) {}},
		{name: "a name that does not parse", change: func(r *objectRef) { r.Name = "seven" },
			wantErr: errCorrupt},
		{name: "epoch zero", change: func(r *objectRef) { r.Name = "0-42" }, wantErr: errCorrupt},
		{name: "a name at another position", change: func(r *objectRef) { r.Position = 43 },
			wantErr: errCorrupt},
		{name: "no nuid", change: func(r *objectRef) { r.NUID = "" }, wantErr: errCorrupt},
		{name: "empty", change: func(r *objectRef) { r.Size = 0 }, wantErr: errCorrupt},
		{name: "larger than the bucket", change: func(r *objectRef) { r.Size = testMaxBytes + 1 },
			wantErr: errCorrupt},
		{name: "a digest without its prefix", change: func(r *objectRef) { r.Digest = r.Digest[len(digestPrefix):] },
			wantErr: errCorrupt},
		{name: "a short digest", change: func(r *objectRef) { r.Digest = digestPrefix + "AAAA" },
			wantErr: errCorrupt},
		{name: "not json", data: "{", wantErr: errCorrupt},
		{name: "a bad stream creation time", data: `{"v":1,"epoch":1,"stream_created":"yesterday"}`,
			wantErr: errCorrupt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := []byte(tt.data)
			if tt.data == "" {
				ref := good
				tt.change(&ref)
				encoded, err := encodePointer(&pointerValue{Version: pointerVersion, Epoch: testEpoch, Previous: &ref})
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				data = encoded
			}

			_, err := decodePointer(data, 1, testMaxBytes)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("decode: err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestMetaWithAHeaderThatDoesNotParseIsCorrupt(t *testing.T) {
	headers := headersOf(&storedObject{epoch: testEpoch, position: 3})
	headers.Set(headerRecordVersion, "256")

	_, err := decodeMeta(headers, testPartition)

	if !errors.Is(err, errCorrupt) || !errors.Is(err, errBadHeader) {
		t.Fatalf("decodeMeta: err = %v, want %v", err, errCorrupt)
	}
}

func TestATimeOutOfRangeIsNoTime(t *testing.T) {
	if got := unixTime(1 << 63); !got.IsZero() {
		t.Fatalf("unixTime(2^63) = %s, want the zero time", got)
	}
}

func TestEveryFailureLeavesTheStoreInTheVocabularyOfProcessing(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []error
		not  []error
	}{
		{name: "a moved pointer is retried", err: errPointerMoved,
			want: []error{processing.ErrCheckpointMoved, errPointerMoved}},
		{name: "a changed object is retried", err: errObjectChanged, want: []error{processing.ErrCheckpointMoved}},
		{name: "a missing checkpoint", err: errCheckpointMissing, want: []error{processing.ErrCheckpointUnusable}},
		{name: "a recreated stream", err: errStreamRecreated, want: []error{processing.ErrCheckpointUnusable}},
		{name: "corruption", err: errCorrupt, want: []error{processing.ErrCheckpointUnusable, errCorrupt},
			not: []error{processing.ErrCheckpointUnavailable}},
		{name: "a later epoch", err: errEpochRegressed, want: []error{processing.ErrCheckpointRefused}},
		{name: "use out of order", err: errStoreState, want: []error{processing.ErrCheckpointRefused}},
		{name: "a closed store", err: errStoreClosed, want: []error{processing.ErrCheckpointRefused}},
		{name: "a discard target that does not match", err: errDiscardTarget,
			want: []error{processing.ErrDiscardRefused}},
		{name: "a lost lease", err: errLeaseLost, want: []error{processing.ErrLeaseLost}},
		{name: "no checkpoint", err: processing.ErrNoCheckpoint, want: []error{processing.ErrNoCheckpoint},
			not: []error{processing.ErrCheckpointUnavailable}},
		{name: "another partition", err: processing.ErrForeignCheckpoint, want: []error{processing.ErrForeignCheckpoint}},
		{name: "a timeout", err: nats.ErrTimeout,
			want: []error{processing.ErrCheckpointUnavailable, context.DeadlineExceeded, nats.ErrTimeout}},
		{name: "a deadline", err: context.DeadlineExceeded,
			want: []error{processing.ErrCheckpointUnavailable, context.DeadlineExceeded}},
		{name: "a cancel stays a cancel", err: context.Canceled, want: []error{context.Canceled},
			not: []error{context.DeadlineExceeded, processing.ErrCheckpointUnavailable}},
		{name: "anything else", err: errInjected, want: []error{processing.ErrCheckpointUnavailable, errInjected}},
		{name: "a failure that wraps EOF never reads as the end of the image", err: fmt.Errorf("conn: %w", io.EOF),
			want: []error{processing.ErrCheckpointUnavailable}, not: []error{io.EOF}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := translate(tt.err)

			for _, want := range tt.want {
				if !errors.Is(got, want) {
					t.Errorf("translate(%v) = %v, want it to be %v", tt.err, got, want)
				}
			}
			for _, not := range tt.not {
				if errors.Is(got, not) {
					t.Errorf("translate(%v) = %v, want it not to be %v", tt.err, got, not)
				}
			}
		})
	}
}

func TestEveryCauseOfTheStoreHasAProcessingMeaning(t *testing.T) {
	causes := []error{errEpochRegressed, errPointerMoved, errObjectChanged, errCheckpointMissing, errStreamRecreated,
		errCorrupt, errStoreClosed, errStoreState, errDiscardTarget, errLeaseLost, errPointerForeign, errCASUnknown,
		errPurgeLeft}
	for _, cause := range causes {
		if !slices.ContainsFunc(translations(), func(t translation) bool { return errors.Is(t.cause, cause) }) {
			t.Errorf("cause %v has no row in translations", cause)
		}
	}
	if len(translations()) != len(causes) {
		t.Errorf("translations has %d rows, the store has %d causes", len(translations()), len(causes))
	}
}
func bucketFor(worst int64) int64 {
	return (worst*bucketFactorTenths + tenths - 1) / tenths
}

func TestACapacityCheckDoesNotOverflow(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		wantErr error
	}{
		{
			name:    "a bucket of all of int64 under 4.4 images of a quarter of it",
			options: Options{MaxBytes: math.MaxInt64, WorstImage: math.MaxInt64 / 4},
			wantErr: ErrInvalidOptions,
		},
		{
			name:    "a bucket of all of int64 over 4.4 images of a fifth of it",
			options: Options{MaxBytes: math.MaxInt64, WorstImage: math.MaxInt64 / 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.options.validateCapacity(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("validateCapacity: err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
