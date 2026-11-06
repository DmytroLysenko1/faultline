package otlp

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	traceparentHeader    = "traceparent"
	traceparentSeparator = "-"
	traceparentVersion   = "00"
	traceparentParts     = 4
	traceIDHexLength     = 32
	parentIDHexLength    = 16
	flagsHexLength       = 2
	lowerHexDigits       = "0123456789abcdef"
	zeroDigit            = "0"
)

func traceIDFrom(ctx context.Context) (string, bool) {
	headers := metadata.ValueFromIncomingContext(ctx, traceparentHeader)
	if len(headers) == 0 {
		return "", false
	}
	return parseTraceparent(headers[0])
}

func parseTraceparent(header string) (string, bool) {
	parts := strings.Split(header, traceparentSeparator)
	if len(parts) != traceparentParts {
		return "", false
	}
	version, traceID, parentID, flags := parts[0], parts[1], parts[2], parts[3]
	if !validTraceparent(version, traceID, parentID, flags) {
		return "", false
	}
	return traceID, true
}

func validTraceparent(version, traceID, parentID, flags string) bool {
	return version == traceparentVersion &&
		isLowerHex(flags, flagsHexLength) &&
		isNonZeroID(traceID, traceIDHexLength) &&
		isNonZeroID(parentID, parentIDHexLength)
}

func isNonZeroID(value string, length int) bool {
	return isLowerHex(value, length) && strings.Trim(value, zeroDigit) != ""
}

func isLowerHex(value string, length int) bool {
	return len(value) == length && strings.Trim(value, lowerHexDigits) == ""
}
