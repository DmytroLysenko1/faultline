package otlp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/test/bufconn"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/operation"
	"github.com/DmytroLysenko1/faultline/internal/domain/telemetry/span"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/decodelimit"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/spanmap"
)

var exportSizes = []int{16, 128, 512, 2048}

const exportBatch = 512

type countingJournal struct {
	spans atomic.Int64
}

func (j *countingJournal) Append(_ context.Context, spans []span.Span) error {
	j.spans.Add(int64(len(spans)))
	return nil
}

func demoRequest(tb testing.TB, count int) ptraceotlp.ExportRequest {
	tb.Helper()
	lines := otlptest.DemoLines(tb)
	traces := ptrace.NewTraces()
	taken := 0
	for line := 0; taken < count; line++ {
		for _, resourceSpans := range lines[line%len(lines)].ResourceSpans().All() {
			if taken == count {
				break
			}
			copied := traces.ResourceSpans().AppendEmpty()
			resourceSpans.CopyTo(copied)
			taken += keepAtMost(copied, count-taken)
		}
	}
	return ptraceotlp.NewExportRequestFromTraces(traces)
}

func keepAtMost(resourceSpans ptrace.ResourceSpans, room int) int {
	kept := 0
	for _, scope := range resourceSpans.ScopeSpans().All() {
		scope.Spans().RemoveIf(func(ptrace.Span) bool {
			kept++
			return kept > room
		})
	}
	return min(kept, room)
}

func newAcceptor(tb testing.TB, journal *countingJournal) *ingest.AcceptBatch {
	tb.Helper()
	rules, err := operation.NewRules(nil)
	if err != nil {
		tb.Fatalf("NewRules: %v", err)
	}
	return ingest.NewAcceptBatch(journal, operation.NewNormalizer(rules))
}

func benchServer(b *testing.B, acceptor *ingest.AcceptBatch) ptraceotlp.GRPCClient {
	b.Helper()
	listener := bufconn.Listen(bufferBytes)
	clock := &fakeClock{
		now: time.Unix(0, 0),
	}
	settings := otlp.DefaultOptions(slog.New(slog.DiscardHandler), clock)
	server := otlp.NewServer(acceptor, otlp.Guards{
		Tap:  otlp.NewTap(otlptest.Admitting(), settings),
		Gate: otlptest.Gate(b, otlp.AdmissionLimits(otlp.MaxRequestsPerSecond)),
	}, settings)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	conn, err := grpc.NewClient(bufconnAddr,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		otlptest.WithTestToken(),
	)
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	b.Cleanup(func() {
		if err := conn.Close(); err != nil {
			b.Errorf("close client: %v", err)
		}
		server.Stop()
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			b.Errorf("Serve: %v", err)
		}
	})
	return ptraceotlp.NewGRPCClient(conn)
}

func BenchmarkExport(b *testing.B) {
	for _, size := range exportSizes {
		b.Run(fmt.Sprintf("spans=%d", size), func(b *testing.B) {
			journal := &countingJournal{}
			client := benchServer(b, newAcceptor(b, journal))
			request := demoRequest(b, size)
			b.ReportAllocs()
			for b.Loop() {
				response, err := client.Export(b.Context(), request, grpc.UseCompressor(grpcgzip.Name))
				if err != nil {
					b.Fatalf("Export: %v", err)
				}
				if rejected := response.PartialSuccess().RejectedSpans(); rejected != 0 {
					b.Fatalf("Export rejected %d spans", rejected)
				}
			}
			if got := journal.spans.Load(); got != int64(size*b.N) {
				b.Fatalf("journal got %d spans, want %d", got, size*b.N)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(size*b.N), "ns/span")
		})
	}
}

func BenchmarkExportServerSide(b *testing.B) {
	for _, size := range exportSizes {
		b.Run(fmt.Sprintf("spans=%d", size), func(b *testing.B) {
			wire, compressed := compressedRequest(b, demoRequest(b, size))
			journal := &countingJournal{}
			ingester := newServerSide(b, journal)
			b.ReportAllocs()
			for b.Loop() {
				if err := ingester.accept(compressed); err != nil {
					b.Fatalf("accept: %v", err)
				}
			}
			if got := journal.spans.Load(); got != int64(size*b.N) {
				b.Fatalf("journal got %d spans, want %d", got, size*b.N)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(size*b.N), "ns/span")
			b.ReportMetric(float64(len(wire))/float64(size), "wire-B/span")
			b.ReportMetric(float64(len(compressed))/float64(size), "gzip-B/span")
		})
	}
}

func BenchmarkExportServerSideParallel(b *testing.B) {
	_, compressed := compressedRequest(b, demoRequest(b, exportBatch))
	journal := &countingJournal{}
	acceptor := newAcceptor(b, journal)
	b.ReportAllocs()
	b.RunParallel(func(workers *testing.PB) {
		ingester := serverSide{
			acceptor: acceptor,
			codec:    newCodec(),
		}
		for workers.Next() {
			if err := ingester.accept(compressed); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(journal.spans.Load())/b.Elapsed().Seconds(), "spans/s")
}

type serverSide struct {
	acceptor *ingest.AcceptBatch
	codec    *decodelimit.Codec
	reader   *gzip.Reader
	raw      bytes.Buffer
}

func newServerSide(tb testing.TB, journal *countingJournal) *serverSide {
	tb.Helper()
	return &serverSide{
		acceptor: newAcceptor(tb, journal),
		codec:    newCodec(),
	}
}

func newCodec() *decodelimit.Codec {
	return decodelimit.NewCodec(decodelimit.NewWeigher(decodelimit.Limits{
		MaxDecodedBytes: otlp.MaxDecodedBytes,
		MaxNestingDepth: otlp.MaxNestingDepth,
	}), func(error, int64) {})
}

func (s *serverSide) accept(compressed []byte) error {
	if err := s.gunzip(compressed); err != nil {
		return err
	}
	request := ptraceotlp.NewExportRequest()
	if err := s.codec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(s.raw.Bytes())}, &request); err != nil {
		return err
	}
	_, err := s.acceptor.Execute(context.Background(), spanmap.AcceptCommand(request.Traces()))
	return err
}

func (s *serverSide) gunzip(compressed []byte) error {
	s.raw.Reset()
	if err := s.open(compressed); err != nil {
		return err
	}
	if _, err := s.raw.ReadFrom(s.reader); err != nil {
		return err
	}
	return s.reader.Close()
}

func (s *serverSide) open(compressed []byte) error {
	if s.reader != nil {
		return s.reader.Reset(bytes.NewReader(compressed))
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	s.reader = reader
	return err
}

func compressedRequest(tb testing.TB, request ptraceotlp.ExportRequest) ([]byte, []byte) {
	tb.Helper()
	wire, err := request.MarshalProto()
	if err != nil {
		tb.Fatalf("MarshalProto: %v", err)
	}
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write(wire); err != nil {
		tb.Fatalf("gzip: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("gzip close: %v", err)
	}
	return wire, out.Bytes()
}
