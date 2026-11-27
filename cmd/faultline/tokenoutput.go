package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"

	"github.com/DmytroLysenko1/faultline/internal/application/tokenadmin"
	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs"
	"github.com/DmytroLysenko1/faultline/internal/infrastructure/natsjs/tokens"
)

const (
	tokenFileMode  = 0o600
	tokenFileFlags = os.O_CREATE | os.O_EXCL | os.O_WRONLY
	sharedModeBits = 0o077

	hintRevokeLive = "the token may be live: run faultline tokens revoke %s, then faultline tokens list"
	hintInitBucket = "run faultline tokens init to create the token bucket"
	hintRecreate   = "follow the runbook Tokens / recreate the bucket"
	hintCheckList  = "check faultline tokens list"
	hintMaxPayload = "raise max_payload on the NATS server: tokens refuses a server that every faultline role refuses"
	failurePrefix  = "faultline tokens: "
	hintRemoveFile = "remove %s if it is empty"
)

var (
	errSharedStdout = errors.New("pass --output-file <path> instead of --stdout")
	errCanceledShow = errors.New("the command was stopped before the token was shown")
)

type tokenFile interface {
	io.Writer
	Sync() error
	Close() error
}

type tokenHint struct {
	target error
	text   string
}

type tokenOutput struct {
	stderr    io.Writer
	stdout    *os.File
	path      string
	file      tokenFile
	syncDir   func(path string) error
	announced credential.ID
}

func openTokenOutput(current *process, request *tokensRequest) (*tokenOutput, error) {
	out := &tokenOutput{
		stderr:  current.Stderr,
		stdout:  current.Stdout,
		path:    request.file,
		syncDir: syncDirectory,
	}
	if request.file == "" {
		return out, nil
	}
	file, err := os.OpenFile( //nolint:gosec // G304: the operator names the token file
		request.file,
		tokenFileFlags,
		tokenFileMode,
	)
	if err != nil {
		return nil, createError(request.file, err)
	}
	out.file = file
	return out, nil
}

func createError(path string, err error) error {
	created := fmt.Errorf("create %s: %w", path, err)
	if !errors.Is(err, fs.ErrExist) {
		return created
	}
	return fmt.Errorf("%w\n"+hintRemoveFile, created, path)
}

func privateStdout(stdout *os.File) error {
	info, err := stdout.Stat()
	if err != nil {
		return fmt.Errorf("stat stdout: %w", err)
	}
	if info.Mode().IsRegular() && info.Mode().Perm()&sharedModeBits != 0 {
		return fmt.Errorf("stdout is a file with mode %s that others can read: %w", info.Mode().Perm(), errSharedStdout)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path) //nolint:gosec // G304: the directory of the token file the operator named
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	if err := errors.Join(directory.Sync(), directory.Close()); err != nil {
		return fmt.Errorf("sync %s: %w", path, err)
	}
	return nil
}

func (o *tokenOutput) Announce(_ context.Context, id credential.ID, tenant string) error {
	o.announced = id
	if _, err := fmt.Fprintf(o.stderr, "issuing ingest token %s for tenant %s\n", id, tenant); err != nil {
		return fmt.Errorf("announce on stderr: %w", err)
	}
	return nil
}

func (o *tokenOutput) Show(ctx context.Context, token credential.IngestToken) error {
	if err := ctx.Err(); err != nil {
		return canceledShow(err)
	}
	shown := credential.Reveal(token) + "\n"
	if o.file == nil {
		return showOnStdout(ctx, o.stdout, shown)
	}
	return o.showInFile(shown)
}

func (o *tokenOutput) showInFile(shown string) error {
	file := o.file
	o.file = nil
	if err := o.storeDurably(file, shown); err != nil {
		return errors.Join(fmt.Errorf("write %s: %w", o.path, err), os.Remove(o.path))
	}
	return nil
}

func (o *tokenOutput) storeDurably(file tokenFile, shown string) error {
	if err := writeDurably(file, shown); err != nil {
		return err
	}
	return o.syncDir(filepath.Dir(o.path))
}

func writeDurably(file tokenFile, shown string) error {
	if _, err := io.WriteString(file, shown); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	return file.Close()
}

func (o *tokenOutput) release() error {
	if o.file == nil {
		return nil
	}
	file := o.file
	o.file = nil
	if err := errors.Join(file.Close(), os.Remove(o.path)); err != nil {
		return fmt.Errorf("remove the unused %s: %w", o.path, err)
	}
	return nil
}

func canceledShow(cause error) error {
	return fmt.Errorf("show the token: %w", errors.Join(errCanceledShow, cause))
}

func showOnStdout(ctx context.Context, stdout io.Writer, shown string) error {
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(stdout, shown)
		written <- err
	}()
	select {
	case err := <-written:
		return shownOnStdout(ctx, err)
	case <-ctx.Done():
		return canceledShow(ctx.Err())
	}
}

func shownOnStdout(ctx context.Context, writeErr error) error {
	if err := ctx.Err(); err != nil {
		return canceledShow(err)
	}
	if writeErr != nil {
		return fmt.Errorf("show on stdout: %w", writeErr)
	}
	return nil
}

func (o *tokenOutput) list(grants []credential.Grant) error {
	for index := range grants {
		grant := &grants[index]
		_, err := fmt.Fprintf(o.stdout, "%s\t%s\t%s\n", grant.ID, grant.Tenant, grant.IssuedAt.UTC().Format(time.RFC3339))
		if err != nil {
			return fmt.Errorf("list on stdout: %w", err)
		}
	}
	return nil
}

func (o *tokenOutput) note(format string, args ...any) error {
	if _, err := fmt.Fprintf(o.stderr, format+"\n", args...); err != nil {
		return fmt.Errorf("write to stderr: %w", err)
	}
	return nil
}

func (o *tokenOutput) failed(err error) int {
	message := failurePrefix + err.Error()
	if hint := o.hint(err); hint != "" {
		message += "\n" + hint
	}
	return failWith(o.stderr, message, exitFailure)
}

func (o *tokenOutput) hint(err error) string {
	found, matched := lo.Find(o.hints(), func(rule tokenHint) bool {
		return errors.Is(err, rule.target)
	})
	if !matched {
		return ""
	}
	return found.text
}

func (o *tokenOutput) hints() []tokenHint {
	return []tokenHint{
		{
			target: tokenadmin.ErrLiveTokenNotShown,
			text:   fmt.Sprintf(hintRevokeLive, o.announced),
		},
		{
			target: tokens.ErrBucketMissing,
			text:   hintInitBucket,
		},
		{
			target: tokens.ErrNeedsRecreate,
			text:   hintRecreate,
		},
		{
			target: tokens.ErrIrreversible,
			text:   hintRecreate,
		},
		{
			target: tokens.ErrBucketDrift,
			text:   hintBucketDrift,
		},
		{
			target: tokenadmin.ErrUnknownToken,
			text:   hintCheckList,
		},
		{
			target: natsjs.ErrPayloadLimit,
			text:   hintMaxPayload,
		},
	}
}
