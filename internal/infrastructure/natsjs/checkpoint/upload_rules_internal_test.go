package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
)

const (
	ownEpoch   = 7
	ownAt      = 20
	paceRate   = 1000
	paceBurst  = 500
	paceChunk  = 500
	stepPeriod = 250 * time.Millisecond
)

var created = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func ownNext() *objectRef {
	return &objectRef{Name: "7-20", NUID: "OWN", Digest: "d", Position: ownAt}
}

func pointerAt(epoch uint64, current *objectRef, streamCreated time.Time) *pointer {
	return &pointer{
		value:   pointerValue{Version: pointerVersion, Epoch: epoch, Current: current},
		created: streamCreated,
	}
}

type judgeArgs struct {
	found *pointer
}

func TestJudgeBeforeDecidesWhetherThisHolderMayWriteThePointer(t *testing.T) {
	tests := []struct {
		name string
		args judgeArgs
		want verdict
	}{
		{name: "no key", args: judgeArgs{}, want: verdictForeign},
		{name: "a pointer that does not parse", args: judgeArgs{found: &pointer{corrupt: true}}, want: verdictForeign},
		{name: "an earlier epoch", args: judgeArgs{found: pointerAt(ownEpoch-1, nil, created)}, want: verdictForeign},
		{name: "a later epoch", args: judgeArgs{found: pointerAt(ownEpoch+1, nil, created)}, want: verdictLeaseLost},
		{
			name: "past this position on the same stream",
			args: judgeArgs{found: pointerAt(ownEpoch, &objectRef{Position: ownAt + 1}, created)},
			want: verdictRegressed,
		},
		{
			name: "past this position on another stream",
			args: judgeArgs{found: pointerAt(ownEpoch, &objectRef{Position: ownAt + 1}, created.Add(time.Hour))},
			want: verdictProceed,
		},
		{
			name: "at this position on the same stream",
			args: judgeArgs{found: pointerAt(ownEpoch, &objectRef{Position: ownAt}, created)},
			want: verdictProceed,
		},
		{name: "own epoch, empty current", args: judgeArgs{found: pointerAt(ownEpoch, nil, created)}, want: verdictProceed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := judgeBefore(tt.args.found, ownEpoch, ownNext(), created); got != tt.want {
				t.Errorf("judgeBefore = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestJudgeAfterDecidesWhatAnUpdateWithoutASuccessDid(t *testing.T) {
	tests := []struct {
		name string
		args judgeArgs
		want verdict
	}{
		{name: "the update landed", args: judgeArgs{found: pointerAt(ownEpoch, ownNext(), created)}, want: verdictAccepted},
		{
			name: "a later epoch took the object over",
			args: judgeArgs{found: pointerAt(ownEpoch+1, ownNext(), created)},
			want: verdictLeaseLost,
		},
		{
			name: "the pointer still names the old current",
			args: judgeArgs{found: pointerAt(ownEpoch, &objectRef{Name: "7-10", NUID: "OLD"}, created)},
			want: verdictUnknown,
		},
		{name: "no key", args: judgeArgs{}, want: verdictUnknown},
		{name: "a pointer that does not parse", args: judgeArgs{found: &pointer{corrupt: true}}, want: verdictUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := judgeAfter(tt.args.found, ownEpoch, ownNext()); got != tt.want {
				t.Errorf("judgeAfter = %d, want %d", got, tt.want)
			}
		})
	}
}

type paceArgs struct {
	after time.Duration
	bytes int64
}

// A burst goes at once; past it every byte waits its share of a second at the rate.
func TestPaceHoldsBytesPastTheBurstToTheRate(t *testing.T) {
	tests := []struct {
		name string
		args []paceArgs
		want []time.Duration
	}{
		{name: "within the burst", args: []paceArgs{{bytes: paceChunk}}, want: []time.Duration{0}},
		{
			name: "past the burst",
			args: []paceArgs{{bytes: paceChunk}, {bytes: paceChunk}},
			want: []time.Duration{0, paceChunk * time.Second / paceRate},
		},
		{
			name: "a pause refills",
			args: []paceArgs{{bytes: paceChunk}, {after: time.Second, bytes: paceChunk}},
			want: []time.Duration{0, 0},
		},
		{
			name: "a long pause refills no more than the burst",
			args: []paceArgs{{bytes: paceChunk}, {after: 2 * time.Second, bytes: paceChunk}, {bytes: paceChunk}},
			want: []time.Duration{0, 0, paceChunk * time.Second / paceRate},
		},
		{
			name: "a short pause refills a part",
			args: []paceArgs{{bytes: paceChunk}, {after: stepPeriod, bytes: paceChunk}},
			want: []time.Duration{0, paceChunk*time.Second/paceRate - stepPeriod},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket := newPace(paceRate, paceBurst, created)
			at := created
			var got []time.Duration
			for _, step := range tt.args {
				at = at.Add(step.after)
				got = append(got, bucket.reserve(at, step.bytes))
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("waits mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestChunkRetryableKeepsOnlyWhatTheBucketRecoversFrom(t *testing.T) {
	tests := []struct {
		name string
		args error
		want bool
	}{
		{name: "a timeout", args: context.DeadlineExceeded, want: true},
		{name: "no responders", args: nats.ErrNoResponders, want: true},
		{name: "disconnected", args: nats.ErrDisconnected, want: true},
		{name: "in process", args: &jetstream.APIError{ErrorCode: 10158}, want: true},
		{name: "not the leader", args: &jetstream.APIError{ErrorCode: 10009}, want: true},
		{name: "too many requests", args: &jetstream.APIError{ErrorCode: 10167}, want: true},
		{name: "a full bucket", args: &jetstream.APIError{ErrorCode: 10077, Description: "maximum bytes exceeded"}},
		{name: "a sealed stream", args: &jetstream.APIError{ErrorCode: 10109}},
		{name: "another stream", args: &jetstream.APIError{ErrorCode: 10060}},
		{name: "too large", args: nats.ErrMaxPayload},
		{name: "canceled", args: context.Canceled},
		{name: "a wrong last sequence", args: &jetstream.APIError{ErrorCode: 10071}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := chunkRetryable(tt.args); got != tt.want {
				t.Errorf("chunkRetryable(%v) = %t, want %t", tt.args, got, tt.want)
			}
		})
	}
}

func TestAMetaWrittenToHeadersReadsBackTheSame(t *testing.T) {
	want := processing.CheckpointMeta{
		Position:          processing.Position{Partition: testPartition, Sequence: 1<<63 + 5},
		ReceivedAt:        created.Add(time.Nanosecond),
		StreamCreated:     created.Add(-time.Hour),
		Epoch:             ownEpoch,
		CheckpointVersion: 1<<32 - 1,
		RecordVersion:     1<<8 - 1,
		LimitsHash:        1<<64 - 1,
		RulesHash:         1<<32 - 2,
	}

	got, err := decodeMeta(encodeMeta(&want), testPartition)
	if err != nil {
		t.Fatalf("decodeMeta: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("meta mismatch (-want +got):\n%s", diff)
	}
}

// Every reason NATS can give maps to a class of a failed upload, so a new reason never goes uncounted.
func TestEveryReasonHasAnUploadClass(t *testing.T) {
	for reason := natsjs.ReasonUnexpected; reason <= natsjs.ReasonClosed; reason++ {
		if _, listed := reasonClasses[reason]; !listed {
			t.Errorf("reason %s has no upload class", reason)
		}
	}
}
