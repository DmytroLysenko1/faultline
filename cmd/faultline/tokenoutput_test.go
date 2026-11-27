package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/interfaces/otlp/otlptest"
)

func shownTestToken() string {
	return otlptest.Token + "\n"
}

type stalledShow struct {
	Canceled    bool
	Nonblocking bool
}

func TestASignalEndsAStalledShowAndLeavesStdoutBlocking(t *testing.T) {
	tests := []struct {
		name string
		args func() ([]int, error)
		want stalledShow
	}{
		{
			name: "a pipe",
			args: pipeDescriptors,
			want: stalledShow{
				Canceled: true,
			},
		},
		{
			name: "a socket",
			args: socketDescriptors,
			want: stalledShow{
				Canceled: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := stalledOutput(t, tt.args)
			ctx, cancel := context.WithCancel(t.Context())
			shown := make(chan error, 1)
			go func() {
				shown <- showOnStdout(ctx, output.stdout, shownTestToken())
			}()

			time.Sleep(showBlocks)
			during := nonblocking(t, output.writer)
			cancel()

			assertDiff(t, "the show and the stdout description", tt.want, stalledShow{
				Canceled:    errors.Is(awaitShow(t, output, shown), errCanceledShow),
				Nonblocking: during,
			})
		})
	}
}

func awaitShow(t *testing.T, output *stalledStdout, shown <-chan error) error {
	t.Helper()
	select {
	case err := <-shown:
		return err
	case <-time.After(commandExitLimit):
		closeErr := output.closeReader()
		t.Fatalf("the show kept writing %s after the signal: %v; close the reader: %v", commandExitLimit, <-shown, closeErr)
	}
	return nil
}

func TestAShownTokenLeavesTheStdoutDescriptionBlocking(t *testing.T) {
	descriptors, err := pipeDescriptors()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	reader := descriptorFile(t, descriptors[0], "reader")
	writer := descriptorFile(t, descriptors[1], "stdout")
	piped := make(chan []byte, 1)
	go func() {
		read, err := io.ReadAll(reader)
		if err != nil {
			t.Errorf("read the pipe: %v", err)
		}
		piped <- read
	}()

	showErr := showOnStdout(t.Context(), writer, shownTestToken())
	after := nonblocking(t, descriptors[1])
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	read := <-piped
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}

	assertDiff(t, "the show, the description and the pipe", []any{true, false, shownTestToken()}, []any{
		showErr == nil,
		after,
		string(read),
	})
}

type tiedContext struct{}

func (tiedContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (tiedContext) Done() <-chan struct{} {
	return nil
}

func (tiedContext) Err() error {
	return context.Canceled
}

func (tiedContext) Value(any) any {
	return nil
}

type acceptingWriter struct {
	written []byte
}

func (w *acceptingWriter) Write(p []byte) (int, error) {
	w.written = append(w.written, p...)
	return len(p), nil
}

func TestASignalWinsATieWithAFinishedShow(t *testing.T) {
	stdout := &acceptingWriter{}

	showErr := showOnStdout(tiedContext{}, stdout, shownTestToken())

	assertDiff(t, "a canceled show and what was written", []any{true, true, shownTestToken()}, []any{
		errors.Is(showErr, errCanceledShow),
		errors.Is(showErr, context.Canceled),
		string(stdout.written),
	})
}

func TestACanceledCommandWritesNoTokenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), tokenFileName)
	out, err := openTokenOutput(&process{}, &tokensRequest{
		file: path,
	})
	if err != nil {
		t.Fatalf("openTokenOutput: %v", err)
	}
	token, err := credential.Parse(otlptest.Token)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	showErr := out.Show(ctx, token)
	written := contentsOf(t, path)
	releaseErr := out.release()
	_, statErr := os.Stat(path)

	assertDiff(t, "a canceled show, the file before and after the release", []any{true, "", nil, true}, []any{
		errors.Is(showErr, errCanceledShow),
		written,
		releaseErr,
		errors.Is(statErr, fs.ErrNotExist),
	})
}
