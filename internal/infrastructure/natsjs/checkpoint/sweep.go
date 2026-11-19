package checkpoint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/samber/lo"
)

const sweepCallBudget = 5 * time.Second

var (
	errSweepStopped   = errors.New("sweep stopped: lease is not valid")
	errUnreadableMeta = errors.New("checkpoint object meta does not parse")
)

type storedMeta struct {
	subject string
	name    string
	nuid    string
}

type metaBody struct {
	NUID string `json:"nuid"`
}

type bucketView struct {
	subjects   map[string]uint64
	metas      []storedMeta
	unreadable int
}

func (s *Store) sweep(ctx context.Context, written *pointer) {
	if err := s.sweepBucket(ctx, written); err != nil {
		s.stats.sweepFailed(err)
	}
}

func (s *Store) sweepBucket(ctx context.Context, written *pointer) error {
	view, err := s.viewBucket(ctx)
	if err != nil {
		return err
	}
	if err := s.sweepMetas(ctx, view.metas, written); err != nil {
		return err
	}
	if view.unreadable > 0 {
		return fmt.Errorf("%w: %d metas, chunks without a meta are kept", errUnreadableMeta, view.unreadable)
	}
	return s.sweepOrphans(ctx, view.subjects, view.metas)
}

func (s *Store) viewBucket(ctx context.Context) (bucketView, error) {
	var subjects map[string]uint64
	err := sweepCall(ctx, func(call context.Context) error {
		var err error
		subjects, err = s.buckets.subjects(call)
		return err
	})
	if err != nil {
		return bucketView{}, fmt.Errorf("list bucket subjects: %w", err)
	}
	view := bucketView{subjects: subjects}
	if err := s.storedMetas(ctx, &view); err != nil {
		return bucketView{}, err
	}
	return view, nil
}

func (s *Store) storedMetas(ctx context.Context, view *bucketView) error {
	prefix := s.subjectPrefix() + metaSubjectPart
	for subject := range view.subjects {
		encoded, isMeta := strings.CutPrefix(subject, prefix)
		if !isMeta {
			continue
		}
		if err := s.addMeta(ctx, view, subject, encoded); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) addMeta(ctx context.Context, view *bucketView, subject, encoded string) error {
	found, err := s.storedMeta(ctx, subject, encoded)
	if errors.Is(err, errNoMessage) {
		return nil
	}
	if errors.Is(err, errUnreadableMeta) {
		view.unreadable++
		s.stats.unreadable.Add(1)
		return nil
	}
	if err != nil {
		return err
	}
	view.metas = append(view.metas, found)
	return nil
}

func (s *Store) storedMeta(ctx context.Context, subject, encoded string) (storedMeta, error) {
	last, err := s.lastMessage(ctx, subject)
	if err != nil {
		return storedMeta{}, fmt.Errorf("read meta %s: %w", subject, err)
	}
	var body metaBody
	if err := json.Unmarshal(last.Data, &body); err != nil {
		return storedMeta{}, fmt.Errorf("%w: %s: %w", errUnreadableMeta, subject, err)
	}
	name, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil || body.NUID == "" {
		return storedMeta{}, fmt.Errorf("%w: %s: name or nuid: %w", errUnreadableMeta, subject, err)
	}
	return storedMeta{
		subject: subject,
		name:    string(name),
		nuid:    body.NUID,
	}, nil
}

func (s *Store) sweepMetas(ctx context.Context, metas []storedMeta, written *pointer) error {
	for _, meta := range metas {
		if !s.stale(meta, written) {
			continue
		}
		if err := s.purgeObject(ctx, meta); err != nil {
			return err
		}
		s.stats.sweptObjects.Add(1)
	}
	return nil
}

func (s *Store) stale(meta storedMeta, written *pointer) bool {
	epoch, _, named := parseName(meta.name)
	return named && epoch <= s.options.Epoch && !written.holds(meta.name)
}

func (s *Store) purgeObject(ctx context.Context, meta storedMeta) error {
	if err := s.purge(ctx, meta.subject); err != nil {
		return err
	}
	return s.purge(ctx, s.chunkSubject(meta.nuid))
}

func (s *Store) sweepOrphans(ctx context.Context, subjects map[string]uint64, metas []storedMeta) error {
	for _, subject := range s.orphanSubjects(subjects, metas) {
		if err := s.sweepOrphan(ctx, subject); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) orphanSubjects(subjects map[string]uint64, metas []storedMeta) []string {
	owned := lo.SliceToMap(metas, func(meta storedMeta) (string, bool) {
		return s.chunkSubject(meta.nuid), true
	})
	prefix := s.subjectPrefix() + chunkSubjectPart
	return lo.Filter(slices.Collect(maps.Keys(subjects)), func(subject string, _ int) bool {
		return strings.HasPrefix(subject, prefix) && !owned[subject]
	})
}

func (s *Store) sweepOrphan(ctx context.Context, subject string) error {
	last, err := s.lastMessage(ctx, subject)
	if errors.Is(err, errNoMessage) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read orphan %s: %w", subject, err)
	}
	if s.now().Sub(last.Time) <= s.options.orphanAge() {
		return nil
	}
	if err := s.purge(ctx, subject); err != nil {
		return err
	}
	s.stats.sweptOrphans.Add(1)
	return nil
}

func (s *Store) lastMessage(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var last *jetstream.RawStreamMsg
	err := sweepCall(ctx, func(call context.Context) error {
		var err error
		last, err = s.buckets.bucket.GetLastMsgForSubject(call, subject)
		return err
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, errNoMessage
	}
	return last, err
}

func (s *Store) purge(ctx context.Context, subject string) error {
	if !s.lease.Valid() {
		return errSweepStopped
	}
	err := sweepCall(ctx, func(call context.Context) error {
		return s.buckets.bucket.Purge(call, jetstream.WithPurgeSubject(subject))
	})
	if err != nil {
		return fmt.Errorf("purge %s: %w", subject, err)
	}
	return nil
}

func sweepCall(ctx context.Context, call func(call context.Context) error) error {
	budget, cancel := context.WithTimeout(ctx, sweepCallBudget)
	defer cancel()
	return call(budget)
}

func (s *Store) subjectPrefix() string {
	return objSubjectPrefix + s.options.objectBucket()
}

func (s *Store) chunkSubject(nuid string) string {
	return s.subjectPrefix() + chunkSubjectPart + nuid
}
