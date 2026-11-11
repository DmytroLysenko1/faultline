package matcher

const noSlot = -1

type pendingSlot struct {
	half pendingHalf
	prev int
	next int
}

type pendingQueue struct {
	slots []pendingSlot
	head  int
	tail  int
	free  int
	count int
}

func newPendingQueue(capacity int) pendingQueue {
	return pendingQueue{
		slots: make([]pendingSlot, 0, capacity),
		head:  noSlot,
		tail:  noSlot,
		free:  noSlot,
	}
}

func (q *pendingQueue) Len() int {
	return q.count
}

func (q *pendingQueue) Front() (int, bool) {
	return q.head, q.head != noSlot
}

func (q *pendingQueue) At(slot int) *pendingHalf {
	return &q.slots[slot].half
}

func (q *pendingQueue) PushBack(half *pendingHalf) int {
	slot := q.take()
	q.slots[slot] = pendingSlot{
		half: *half,
		prev: q.tail,
		next: noSlot,
	}
	q.setNext(q.tail, slot)
	q.tail = slot
	q.count++
	return slot
}

func (q *pendingQueue) Remove(slot int) pendingHalf {
	removed := q.slots[slot]
	q.unlink(removed.prev, removed.next)
	q.slots[slot] = pendingSlot{
		prev: noSlot,
		next: q.free,
	}
	q.free = slot
	q.count--
	return removed.half
}

func (q *pendingQueue) unlink(prev, next int) {
	q.setNext(prev, next)
	q.setPrev(next, prev)
}

func (q *pendingQueue) setNext(slot, next int) {
	if slot == noSlot {
		q.head = next
		return
	}
	q.slots[slot].next = next
}

func (q *pendingQueue) setPrev(slot, prev int) {
	if slot == noSlot {
		q.tail = prev
		return
	}
	q.slots[slot].prev = prev
}

func (q *pendingQueue) take() int {
	if q.free == noSlot {
		q.slots = append(q.slots, pendingSlot{})
		return len(q.slots) - 1
	}
	slot := q.free
	q.free = q.slots[slot].next
	return slot
}
