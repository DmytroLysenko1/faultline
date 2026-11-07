package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	digits = "0123456789"
	kib    = 1 << 10
	mib    = 1 << 20
	gib    = 1 << 30
	tib    = 1 << 40
)

var (
	errNotDigits   = errors.New("not an unsigned decimal integer")
	errUnknownUnit = errors.New("unknown size unit")
	errOverflow    = errors.New("value overflows int64")
)

type sizeUnit struct {
	suffix string
	bytes  int64
}

func configUnits() []sizeUnit {
	return []sizeUnit{
		{suffix: "GiB", bytes: gib},
		{suffix: "MiB", bytes: mib},
		{suffix: "KiB", bytes: kib},
		{suffix: "B", bytes: 1},
	}
}

func runtimeUnits() []sizeUnit {
	return append([]sizeUnit{
		{suffix: "TiB", bytes: tib},
	}, configUnits()...)
}

func parseUnsigned(raw string) (int64, error) {
	if raw == "" || strings.Trim(raw, digits) != "" {
		return 0, errNotDigits
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", errOverflow, err)
	}
	return value, nil
}

func parseSize(raw string, units []sizeUnit) (int64, error) {
	for _, unit := range units {
		number, found := strings.CutSuffix(raw, unit.suffix)
		if !found {
			continue
		}
		return scale(number, unit.bytes)
	}
	return 0, errUnknownUnit
}

func scale(number string, unit int64) (int64, error) {
	value, err := parseUnsigned(number)
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt64/unit {
		return 0, errOverflow
	}
	return value * unit, nil
}

func formatSize(bytes int64) string {
	for _, unit := range configUnits() {
		if bytes%unit.bytes == 0 && bytes >= unit.bytes {
			return strconv.FormatInt(bytes/unit.bytes, 10) + unit.suffix
		}
	}
	return strconv.FormatInt(bytes, 10) + "B"
}
