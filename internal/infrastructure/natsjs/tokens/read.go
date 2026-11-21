package tokens

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/DmytroLysenko1/faultline/internal/domain/credential"
)

const (
	infoSubject   = "$JS.API.STREAM.INFO."
	peerFreshness = 3 * time.Second
	quorumDivisor = 2
)

var (
	ErrBucketMissing = errors.New("token bucket missing")
	ErrBucketDrift   = errors.New("token bucket configuration drifted")
	ErrNoLeader      = errors.New("token bucket answered without a group leader")
	ErrNoQuorum      = errors.New("token bucket leader does not see a quorum")
	ErrTooManyKeys   = errors.New("token bucket holds more keys than the directory reads")
)

type bucketState struct {
	LastSeq  uint64
	FirstSeq uint64
	Msgs     uint64
	Created  time.Time
}

type bucketEntries struct {
	State   bucketState
	Grants  credential.Grants
	Invalid uint64
}

type infoRequest struct {
	SubjectsFilter string `json:"subjects_filter,omitempty"`
}

type infoResponse struct {
	Error *apiError `json:"error"`
	jetstream.StreamInfo
}

type bucketReader struct {
	nc     *nats.Conn
	stream string
}

func (r bucketReader) readState(ctx context.Context) (bucketState, configVerdict, error) {
	info, verdict, err := r.leaderInfo(ctx, "")
	if err != nil {
		return bucketState{}, verdict, err
	}
	return stateOf(info), verdict, nil
}

func (r bucketReader) readEntries(ctx context.Context) (bucketEntries, configVerdict, error) {
	info, verdict, err := r.leaderInfo(ctx, allSubjects)
	if err != nil {
		return bucketEntries{}, verdict, err
	}
	if info.State.NumSubjects > MaxGrants || len(info.State.Subjects) > MaxGrants {
		return bucketEntries{}, verdict, fmt.Errorf("%d keys, limit %d: %w", info.State.NumSubjects, MaxGrants, ErrTooManyKeys)
	}
	entries, err := r.readKeys(ctx, slices.Sorted(maps.Keys(info.State.Subjects)))
	if err != nil {
		return bucketEntries{}, verdict, err
	}
	entries.State = stateOf(info)
	return entries, verdict, nil
}

func (r bucketReader) readKeys(ctx context.Context, subjects []string) (bucketEntries, error) {
	entries := bucketEntries{
		Grants: credential.Grants{},
	}
	for _, subject := range subjects {
		grant, found, err := r.readKey(ctx, subject)
		if err != nil && !errors.Is(err, errInvalidEntry) {
			return bucketEntries{}, err
		}
		entries.add(&grant, found, err)
	}
	return entries, nil
}

func (e *bucketEntries) add(grant *credential.Grant, found bool, err error) {
	if err != nil {
		e.Invalid++
		return
	}
	if found {
		e.Grants[grant.ID] = *grant
	}
}

func (r bucketReader) readKey(ctx context.Context, subject string) (credential.Grant, bool, error) {
	id, err := idOf(subject)
	if err != nil {
		return credential.Grant{}, false, err
	}
	entry, found, err := r.lookup(ctx, subject)
	if err != nil || !found {
		return credential.Grant{}, false, err
	}
	if !published(entry, id) {
		return credential.Grant{}, false, fmt.Errorf("grant %s not written by a bucket publish: %w", id, errInvalidEntry)
	}
	grant, err := decodeEntry(id, entry.Data)
	return grant, err == nil, err
}

func published(entry rawEntry, id credential.ID) bool {
	want := map[string]string{
		jetstream.MsgIDHeader:               string(id),
		jetstream.ExpectedStreamHeader:      streamName,
		jetstream.ExpectedLastSubjSeqHeader: strconv.Itoa(emptySubject),
	}
	if len(entry.Header) != len(want) {
		return false
	}
	for key, value := range want {
		if values := entry.Header[key]; len(values) != 1 || values[0] != value {
			return false
		}
	}
	return true
}

func (r bucketReader) lookup(ctx context.Context, subject string) (rawEntry, bool, error) {
	entry, found, err := leaderGet(ctx, r.nc, r.stream, subject)
	if err != nil || !found || removed(entry) {
		return rawEntry{}, false, err
	}
	return entry, true, nil
}

func (r bucketReader) leaderInfo(ctx context.Context, subjects string) (*jetstream.StreamInfo, configVerdict, error) {
	info, err := r.info(ctx, subjects)
	if err != nil {
		return nil, configVerdict{}, err
	}
	if err := leaderWithQuorum(info); err != nil {
		return nil, configVerdict{}, err
	}
	return info, verdictOf(&info.Config), nil
}

func verdictOf(actual *jetstream.StreamConfig) configVerdict {
	return checkConfig(new(desiredConfig(actual.Replicas)), actual)
}

func refuseDrift(verdict *configVerdict) error {
	if fields := verdict.blocksReading(); len(fields) > 0 {
		return fmt.Errorf("%s: %w", strings.Join(fields, ","), ErrBucketDrift)
	}
	return nil
}

func (r bucketReader) info(ctx context.Context, subjects string) (*jetstream.StreamInfo, error) {
	var response infoResponse
	if err := request(ctx, r.nc, infoSubject+r.stream, infoRequest{SubjectsFilter: subjects}, &response); err != nil {
		return nil, err
	}
	if response.Error == nil {
		return &response.StreamInfo, nil
	}
	if jetstream.ErrorCode(response.Error.ErrCode) == jetstream.JSErrCodeStreamNotFound {
		return nil, fmt.Errorf("stream %s: %w", r.stream, ErrBucketMissing)
	}
	return nil, fmt.Errorf("info of %s: %w", r.stream, response.Error.asError())
}

func leaderWithQuorum(info *jetstream.StreamInfo) error {
	if info.Cluster == nil || info.Cluster.Leader == "" {
		return fmt.Errorf("stream %s: %w", info.Config.Name, ErrNoLeader)
	}
	seen := 1 + currentPeers(info.Cluster.Replicas)
	if need := info.Config.Replicas/quorumDivisor + 1; seen < need {
		return fmt.Errorf("stream %s leader sees %d of %d needed: %w", info.Config.Name, seen, need, ErrNoQuorum)
	}
	return nil
}

func currentPeers(peers []*jetstream.PeerInfo) int {
	current := 0
	for _, peer := range peers {
		if peerCurrent(peer) {
			current++
		}
	}
	return current
}

func peerCurrent(peer *jetstream.PeerInfo) bool {
	return peer != nil &&
		!peer.Offline &&
		!peer.Pending &&
		peer.Active > 0 &&
		peer.Active <= peerFreshness
}

func stateOf(info *jetstream.StreamInfo) bucketState {
	return bucketState{
		LastSeq:  info.State.LastSeq,
		FirstSeq: info.State.FirstSeq,
		Msgs:     info.State.Msgs,
		Created:  info.Created,
	}
}
