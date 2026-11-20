package checkpoint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	metaIDSuffix       = ".meta"
	rollupSubject      = "sub"
	noEarlierOnSubject = 0
)

var errNameCollision = errors.New("another checkpoint object has this name")

func (s *Store) metaSubject(name string) string {
	return s.subjectPrefix() + metaSubjectPart + base64.URLEncoding.EncodeToString([]byte(name))
}

func (o *object) metaID() string {
	return o.nuid + metaIDSuffix
}

func (o *object) info(bucket string) *jetstream.ObjectInfo {
	return &jetstream.ObjectInfo{
		ObjectMeta: jetstream.ObjectMeta{
			Name:    o.name,
			Headers: encodeMeta(&o.meta),
			Opts: &jetstream.ObjectMetaOptions{
				ChunkSize: o.chunkSize,
			},
		},
		Bucket:  bucket,
		NUID:    o.nuid,
		Size:    unsigned(o.size),
		ModTime: time.Now().UTC(),
		Chunks:  o.chunks,
		Digest:  o.digest,
	}
}

func (u *uploader) sendMeta(ctx context.Context, written *object) error {
	data, err := json.Marshal(written.info(u.store.options.objectBucket()))
	if err != nil {
		return fmt.Errorf("encode the meta of %s: %w", written.name, err)
	}
	msg := &nats.Msg{
		Subject: u.store.metaSubject(written.name),
		Data:    data,
		Header:  nats.Header{nats.MsgRollup: []string{rollupSubject}},
	}
	_, err = u.publish(ctx, msg, jetstream.WithMsgID(written.metaID()),
		jetstream.WithExpectLastSequencePerSubject(noEarlierOnSubject))
	if !sequenceConflict(err) {
		return err
	}
	return u.ownMeta(ctx, written)
}

func sequenceConflict(err error) bool {
	apiErr, isAPI := errors.AsType[*jetstream.APIError](err)
	return isAPI && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

func (u *uploader) ownMeta(ctx context.Context, written *object) error {
	last, err := u.store.lastMessage(ctx, u.store.metaSubject(written.name))
	if err != nil {
		return fmt.Errorf("read the meta of %s after a conflict: %w", written.name, err)
	}
	if last.Header.Get(jetstream.MsgIDHeader) == written.metaID() {
		return nil
	}
	return failed(failNameCollision, fmt.Errorf("%w: %s", errNameCollision, written.name))
}
