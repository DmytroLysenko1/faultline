package otlptest

import (
	"bufio"
	"compress/gzip"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	MaxRequestBytes = 4 << 20
	requestSlack    = 64 << 10
	maxDumpLine     = 64 << 20
	sizeCheckEvery  = 1000
	byteMask        = 0xff
	byteShift       = 8
	thirdByteShift  = 2 * byteShift
	moduleRoot      = "go.mod"
	demoDumpPath    = "test/demo/otel-demo/traces.otlp.json.gz"
)

func HeapInUse() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(min(stats.HeapAlloc, math.MaxInt64))
}

func Message(number protowire.Number, body []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), body)
}

func Repeat(part []byte, count int) []byte {
	out := make([]byte, 0, len(part)*count)
	for range count {
		out = append(out, part...)
	}
	return out
}

func Encode(tb testing.TB, traces ptrace.Traces) []byte {
	tb.Helper()
	wire, err := ptraceotlp.NewExportRequestFromTraces(traces).MarshalProto()
	if err != nil {
		tb.Fatalf("MarshalProto: %v", err)
	}
	return wire
}

func MinimalSpans(tb testing.TB) ptrace.Traces {
	tb.Helper()
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	resourceSpans.Resource().Attributes().PutStr("service.name", "checkout")
	spans := resourceSpans.ScopeSpans().AppendEmpty().Spans()
	sizer := ptrace.ProtoMarshaler{}
	for index := 0; index%sizeCheckEvery != 0 || sizer.TracesSize(traces) < MaxRequestBytes-requestSlack; index++ {
		AppendNumberedSpan(spans, index)
	}
	return traces
}

func AppendNumberedSpan(spans ptrace.SpanSlice, index int) {
	created := spans.AppendEmpty()
	created.SetTraceID(pcommon.TraceID{
		byte(index & byteMask),
		byte(index >> byteShift & byteMask),
		byte(index >> thirdByteShift & byteMask),
		1,
	})
	created.SetSpanID(pcommon.SpanID{
		byte(index & byteMask),
		byte(index >> byteShift & byteMask),
		byte(index >> thirdByteShift & byteMask),
		1,
	})
}

func DemoSpans(tb testing.TB) ptrace.Traces {
	tb.Helper()
	lines := DemoLines(tb)
	traces := ptrace.NewTraces()
	sizer := ptrace.ProtoMarshaler{}
	for index := 0; sizer.TracesSize(traces) < MaxRequestBytes-requestSlack; index++ {
		for _, resourceSpans := range lines[index%len(lines)].ResourceSpans().All() {
			resourceSpans.CopyTo(traces.ResourceSpans().AppendEmpty())
		}
	}
	return traces
}

func DemoLines(tb testing.TB) []ptrace.Traces {
	tb.Helper()
	file, err := os.Open(filepath.Join(ModuleRoot(tb), demoDumpPath))
	if err != nil {
		tb.Fatalf("open demo dump: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			tb.Errorf("close demo dump: %v", err)
		}
	}()
	unzipped, err := gzip.NewReader(file)
	if err != nil {
		tb.Fatalf("gunzip demo dump: %v", err)
	}
	return decodeLines(tb, bufio.NewScanner(unzipped))
}

func decodeLines(tb testing.TB, scanner *bufio.Scanner) []ptrace.Traces {
	tb.Helper()
	scanner.Buffer(make([]byte, 0, maxDumpLine), maxDumpLine)
	var lines []ptrace.Traces
	for scanner.Scan() {
		traces, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(scanner.Bytes())
		if err != nil {
			tb.Fatalf("demo dump line %d: %v", len(lines)+1, err)
		}
		lines = append(lines, traces)
	}
	if err := scanner.Err(); err != nil {
		tb.Fatalf("read demo dump: %v", err)
	}
	return lines
}

func ModuleRoot(tb testing.TB) string {
	tb.Helper()
	directory, err := os.Getwd()
	if err != nil {
		tb.Fatalf("working directory: %v", err)
	}
	for !hasModuleFile(directory) {
		parent := filepath.Dir(directory)
		if parent == directory {
			tb.Fatalf("go.mod not found above the test directory")
		}
		directory = parent
	}
	return directory
}

func hasModuleFile(directory string) bool {
	_, err := os.Stat(filepath.Join(directory, moduleRoot))
	return err == nil
}
