package record

type CorruptReason uint8

const (
	CorruptBody CorruptReason = iota
	CorruptLength
	CorruptMagic
	CorruptChecksum
	CorruptTenant
	CorruptPartition
	CorruptWeight
	CorruptShape
	corruptReasonCount
)

const unknownCorruptReason = "unknown"

var corruptReasonNames = [corruptReasonCount]string{
	CorruptBody:      "body",
	CorruptLength:    "length",
	CorruptMagic:     "magic",
	CorruptChecksum:  "crc",
	CorruptTenant:    "tenant",
	CorruptPartition: "partition",
	CorruptWeight:    "weight",
	CorruptShape:     "shape",
}

func (r CorruptReason) String() string {
	if r >= corruptReasonCount {
		return unknownCorruptReason
	}
	return corruptReasonNames[r]
}

type CorruptError struct {
	Reason CorruptReason
	Cause  error
}

func (e *CorruptError) Error() string {
	return e.Cause.Error()
}

func (e *CorruptError) Unwrap() error {
	return e.Cause
}

func corruptBy(reason CorruptReason, cause error) error {
	return &CorruptError{
		Reason: reason,
		Cause:  cause,
	}
}
