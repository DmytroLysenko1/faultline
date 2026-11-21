package checkpoint

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/DmytroLysenko1/faultline/internal/application/processing"
)

const (
	spoolBuffer = 1 << 20
	spoolSuffix = ".spool"
	spoolFormat = "%d-%d" + spoolSuffix
)

type spool struct {
	name  string
	meta  processing.CheckpointMeta
	size  int64
	final bool
}

type spoolWriter struct {
	up     *uploader
	file   *os.File
	buffer *bufio.Writer
	spool  spool
	closed bool
}

func (w *spoolWriter) Write(data []byte) (int, error) {
	written, err := w.buffer.Write(data)
	w.spool.size += int64(written)
	if err != nil {
		return written, translate(fmt.Errorf("write spool %s: %w", w.spool.name, err))
	}
	return written, nil
}

func (w *spoolWriter) Close() error {
	if w.closed {
		return translate(fmt.Errorf("%w: checkpoint writer closed twice", errStoreState))
	}
	w.closed = true
	if err := errors.Join(w.buffer.Flush(), w.file.Close()); err != nil {
		w.up.release(&w.spool)
		return translate(fmt.Errorf("finish spool %s: %w", w.spool.name, err))
	}
	return translate(w.up.handOff(&w.spool))
}

func (w *spoolWriter) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	err := w.file.Close()
	w.up.release(&w.spool)
	if err != nil {
		return translate(fmt.Errorf("abort spool %s: %w", w.spool.name, err))
	}
	return nil
}

func (s *Store) createSpool(meta *processing.CheckpointMeta) (*os.File, spool, error) {
	name := fmt.Sprintf(spoolFormat, meta.Epoch, meta.Position.Sequence)
	root, err := os.OpenRoot(s.options.SpoolDir)
	if err != nil {
		return nil, spool{}, fmt.Errorf("open spool dir: %w", err)
	}
	file, err := root.Create(name)
	if err = errors.Join(err, root.Close()); err != nil {
		return nil, spool{}, fmt.Errorf("create spool %s: %w", name, errors.Join(err, s.abandon(file, name)))
	}
	return file, spool{
		name: name,
		meta: *meta,
	}, nil
}

func (s *Store) openSpool(name string) (*os.File, error) {
	root, err := os.OpenRoot(s.options.SpoolDir)
	if err != nil {
		return nil, fmt.Errorf("open spool dir: %w", err)
	}
	file, err := root.Open(name)
	if err = errors.Join(err, root.Close()); err != nil {
		return nil, fmt.Errorf("open spool %s: %w", name, err)
	}
	return file, nil
}

func (s *Store) removeSpool(name string) {
	root, err := os.OpenRoot(s.options.SpoolDir)
	if err == nil {
		err = errors.Join(ignoreMissing(root.Remove(name)), root.Close())
	}
	if err != nil {
		s.stats.spoolLeft.Add(1)
	}
}

func ignoreMissing(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func removeEach(root *os.Root, entries []os.DirEntry) error {
	var err error
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), spoolSuffix) {
			err = errors.Join(err, root.Remove(entry.Name()))
		}
	}
	return err
}

func removeSpools(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("%w: spool dir: %w", ErrInvalidOptions, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("%w: spool dir: %w", ErrInvalidOptions, err)
	}
	if err = errors.Join(removeEach(root, entries), root.Close()); err != nil {
		return fmt.Errorf("remove old spools: %w", err)
	}
	return nil
}

func (s *Store) abandon(file *os.File, name string) error {
	if file == nil {
		return nil
	}
	err := file.Close()
	s.removeSpool(name)
	if err != nil {
		return fmt.Errorf("close abandoned spool: %w", err)
	}
	return nil
}
