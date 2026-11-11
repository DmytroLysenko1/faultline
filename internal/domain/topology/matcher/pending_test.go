package matcher

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	queueCapacity = 5
	churnRuns     = 100
	lastDeadline  = 6
	churnPeak     = 4
)

type queueStep struct {
	Push   time.Duration
	Remove time.Duration
}

func halfDue(deadline time.Duration) *pendingHalf {
	return &pendingHalf{
		Deadline: deadline,
	}
}

func replay(queue *pendingQueue, steps []queueStep) {
	var slotByDeadline [lastDeadline + 1]int
	for _, step := range steps {
		if step.Push != 0 {
			slotByDeadline[step.Push] = queue.PushBack(halfDue(step.Push))
		}
		if step.Remove != 0 {
			queue.Remove(slotByDeadline[step.Remove])
		}
	}
}

func drain(queue *pendingQueue) []time.Duration {
	var order []time.Duration
	for {
		slot, found := queue.Front()
		if !found {
			return order
		}
		order = append(order, queue.Remove(slot).Deadline)
	}
}

func TestPendingQueueKeepsArrivalOrderAcrossRemovalsAndReuse(t *testing.T) {
	tests := []struct {
		Name string
		Args []queueStep
		Want []time.Duration
	}{
		{
			Name: "arrival order",
			Args: []queueStep{
				{Push: 1},
				{Push: 2},
				{Push: 3},
			},
			Want: []time.Duration{1, 2, 3},
		},
		{
			Name: "a middle removal and a reused slot",
			Args: []queueStep{
				{Push: 1},
				{Push: 2},
				{Push: 3},
				{Push: 4},
				{Remove: 2},
				{Push: 5},
				{Remove: 4},
				{Push: 6},
			},
			Want: []time.Duration{1, 3, 5, 6},
		},
		{
			Name: "the head and the tail removed",
			Args: []queueStep{
				{Push: 1},
				{Push: 2},
				{Push: 3},
				{Remove: 1},
				{Remove: 3},
				{Push: 4},
			},
			Want: []time.Duration{2, 4},
		},
		{
			Name: "emptied and refilled",
			Args: []queueStep{
				{Push: 1},
				{Remove: 1},
				{Push: 2},
				{Push: 3},
			},
			Want: []time.Duration{2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			queue := newPendingQueue(queueCapacity)
			replay(&queue, tt.Args)
			if got := queue.Len(); got != len(tt.Want) {
				t.Errorf("Len = %d, want %d", got, len(tt.Want))
			}
			if diff := cmp.Diff(tt.Want, drain(&queue)); diff != "" {
				t.Errorf("order (-want +got):\n%s", diff)
			}
		})
	}
}

// TestPendingQueueReusesItsSlotsWithoutAllocating: churn within the capacity only reuses freed
// slots, so the queue never grows past the slots it was given up front.
func TestPendingQueueReusesItsSlotsWithoutAllocating(t *testing.T) {
	queue := newPendingQueue(queueCapacity)
	churn := []queueStep{
		{Push: 1},
		{Push: 2},
		{Push: 3},
		{Push: 4},
		{Remove: 2},
		{Remove: 3},
		{Push: 5},
		{Push: 6},
		{Remove: 1},
		{Remove: 5},
		{Remove: 4},
		{Remove: 6},
	}
	allocations := testing.AllocsPerRun(churnRuns, func() {
		replay(&queue, churn)
	})
	if allocations != 0 {
		t.Errorf("churn allocated %.1f times per run, want 0", allocations)
	}
	if diff := cmp.Diff([2]int{churnPeak, queueCapacity}, [2]int{len(queue.slots), cap(queue.slots)}); diff != "" {
		t.Errorf("slots used and held after churn (-want +got):\n%s", diff)
	}
}
