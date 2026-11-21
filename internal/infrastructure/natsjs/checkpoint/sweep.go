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

const (
	sweepCallBudget = 5 * time.Second
	sweepBudget     = 10 * time.Second
)

var (
	errSweepLeaseLost = errors.New("checkpoint sweep stopped: the partition lease is not valid")
	errUnreadableMeta = errors.New("checkpoint object meta does not parse")
)

type storedMeta struct {
	subject string
	name    string
	nuid    string
	size    uint64
}

type metaBody struct {
	NUID string `json:"nuid"`
	Size uint64 `json:"size"`
}

type bucketView struct {
	subjects   map[string]uint64
	metas      []storedMeta
	unreadable uint64
}

type sweepRule struct {
	pointer *pointer
	below   uint64
	proceed func() error
}

type sweepOutcome struct {
	completed   bool
	removed     int
	firstMature time.Time
}

type sweeper struct {
	store   *Store
	rule    *sweepRule
	outcome sweepOutcome
}

func stakingRule(written *pointer, own uint64, proceed func() error) *sweepRule {
	return &sweepRule{
		pointer: written,
		below:   own + 1,
		proceed: proceed,
	}
}

func spaceRule(found *pointer, own uint64, proceed func() error) *sweepRule {
	return &sweepRule{
		pointer: found,
		below:   own,
		proceed: proceed,
	}
}

func (s *Store) sweep(ctx context.Context, written *pointer) {
	_, err := s.sweepBucket(ctx, stakingRule(written, s.options.Epoch, s.leaseKept))
	s.stats.sweepEnded(err)
}

func (s *Store) leaseKept() error {
	if !s.lease.Valid() {
		return errSweepLeaseLost
	}
	return nil
}

func (s *Store) sweepBucket(ctx context.Context, rule *sweepRule) (sweepOutcome, error) {
	ctx, cancel := context.WithTimeout(ctx, sweepBudget)
	defer cancel()
	run := &sweeper{
		store: s,
		rule:  rule,
	}
	err := run.sweep(ctx)
	return run.outcome, err
}

func (r *sweepRule) stale(meta *storedMeta) bool {
	epoch, _, named := parseName(meta.name)
	return named && epoch < r.below && !r.pointer.holds(meta.name)
}

func (o *sweepOutcome) matures(at time.Time) {
	if o.firstMature.IsZero() || at.Before(o.firstMature) {
		o.firstMature = at
	}
}

func (o *sweepOutcome) pausedUntil(now time.Time, orphanAge time.Duration) time.Time {
	if !o.completed || o.removed > 0 {
		return time.Time{}
	}
	if o.firstMature.IsZero() {
		return now.Add(orphanAge)
	}
	return o.firstMature
}

func (w *sweeper) sweep(ctx context.Context) error {
	view, err := w.view(ctx)
	if err != nil {
		return err
	}
	if err := w.sweepMetas(ctx, view.metas); err != nil {
		return err
	}
	if view.unreadable > 0 {
		w.outcome.completed = true
		return fmt.Errorf("%w: %d metas, chunks without a meta are kept", errUnreadableMeta, view.unreadable)
	}
	if err := w.sweepOrphans(ctx, view.subjects, view.metas); err != nil {
		return err
	}
	w.outcome.completed = true
	return nil
}

func (w *sweeper) view(ctx context.Context) (bucketView, error) {
	var subjects map[string]uint64
	err := w.call(ctx, func(call context.Context) error {
		var err error
		subjects, err = w.store.buckets.subjects(call)
		return err
	})
	if err != nil {
		return bucketView{}, fmt.Errorf("list bucket subjects: %w", err)
	}
	view := bucketView{subjects: subjects}
	if err := w.storedMetas(ctx, &view); err != nil {
		return bucketView{}, err
	}
	w.store.stats.unreadable.Store(view.unreadable)
	return view, nil
}

func (w *sweeper) storedMetas(ctx context.Context, view *bucketView) error {
	prefix := w.store.subjectPrefix() + metaSubjectPart
	for subject := range view.subjects {
		encoded, isMeta := strings.CutPrefix(subject, prefix)
		if !isMeta {
			continue
		}
		if err := w.addMeta(ctx, view, subject, encoded); err != nil {
			return err
		}
	}
	return nil
}

func (w *sweeper) addMeta(ctx context.Context, view *bucketView, subject, encoded string) error {
	found, err := w.storedMeta(ctx, subject, encoded)
	if errors.Is(err, errNoMessage) {
		return nil
	}
	if errors.Is(err, errUnreadableMeta) {
		view.unreadable++
		return nil
	}
	if err != nil {
		return err
	}
	view.metas = append(view.metas, found)
	return nil
}

func (w *sweeper) storedMeta(ctx context.Context, subject, encoded string) (storedMeta, error) {
	last, err := w.lastMessage(ctx, subject)
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
		size:    body.Size,
	}, nil
}

func (w *sweeper) sweepMetas(ctx context.Context, metas []storedMeta) error {
	for _, meta := range metas {
		if !w.rule.stale(&meta) {
			continue
		}
		if err := w.purgeObject(ctx, &meta); err != nil {
			return err
		}
	}
	return nil
}

func (w *sweeper) purgeObject(ctx context.Context, meta *storedMeta) error {
	if err := w.purge(ctx, meta.subject); err != nil {
		return err
	}
	w.outcome.removed++
	w.store.stats.sweptObjects.Add(1)
	w.store.stats.sweptBytes.Add(meta.size)
	return w.purge(ctx, w.store.chunkSubject(meta.nuid))
}

func (w *sweeper) sweepOrphans(ctx context.Context, subjects map[string]uint64, metas []storedMeta) error {
	for _, subject := range w.store.orphanSubjects(subjects, metas) {
		if err := w.sweepOrphan(ctx, subject); err != nil {
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

func (w *sweeper) sweepOrphan(ctx context.Context, subject string) error {
	last, err := w.lastMessage(ctx, subject)
	if errors.Is(err, errNoMessage) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read orphan %s: %w", subject, err)
	}
	mature := last.Time.Add(w.store.options.orphanAge())
	if !w.store.now().After(mature) {
		w.outcome.matures(mature)
		return nil
	}
	if err := w.purge(ctx, subject); err != nil {
		return err
	}
	w.outcome.removed++
	w.store.stats.sweptOrphans.Add(1)
	return nil
}

func (w *sweeper) lastMessage(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var last *jetstream.RawStreamMsg
	err := w.call(ctx, func(call context.Context) error {
		var err error
		last, err = w.store.buckets.bucket.GetLastMsgForSubject(call, subject)
		return err
	})
	return last, noMessage(err)
}

func (w *sweeper) purge(ctx context.Context, subject string) error {
	err := w.call(ctx, func(call context.Context) error {
		return w.store.buckets.bucket.Purge(call, jetstream.WithPurgeSubject(subject))
	})
	if err != nil {
		return fmt.Errorf("purge %s: %w", subject, err)
	}
	return nil
}

func (w *sweeper) call(ctx context.Context, call func(call context.Context) error) error {
	if err := w.rule.proceed(); err != nil {
		return err
	}
	return stopMarked(ctx, sweepCall(ctx, call))
}

func (s *Store) lastMessage(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var last *jetstream.RawStreamMsg
	err := sweepCall(ctx, func(call context.Context) error {
		var err error
		last, err = s.buckets.bucket.GetLastMsgForSubject(call, subject)
		return err
	})
	return last, noMessage(err)
}

func noMessage(err error) error {
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return errNoMessage
	}
	return err
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
