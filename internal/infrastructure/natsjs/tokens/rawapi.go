package tokens

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	msgGetSubject   = "$JS.API.STREAM.MSG.GET."
	purgeSubject    = "$JS.API.STREAM.PURGE."
	headerStatus    = "NATS/1.0"
	headerLineEnd   = "\r\n"
	headerSeparator = ":"
	keyBlanks       = " \t"
	bareLineBreaks  = "\r\n"
)

type rawEntry struct {
	Sequence uint64
	Header   nats.Header
	Data     []byte
}

type apiError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code"`
	Description string `json:"description"`
}

type storedMessage struct {
	Subject  string `json:"subject"`
	Sequence uint64 `json:"seq"`
	Header   []byte `json:"hdrs"`
	Data     []byte `json:"data"`
}

type msgGetRequest struct {
	LastFor string `json:"last_by_subj"`
}

type msgGetResponse struct {
	Error   *apiError      `json:"error"`
	Message *storedMessage `json:"message"`
}

type purgeRequest struct {
	Filter string `json:"filter"`
}

type purgeResponse struct {
	Error   *apiError `json:"error"`
	Success bool      `json:"success"`
	Purged  uint64    `json:"purged"`
}

func leaderGet(ctx context.Context, nc *nats.Conn, stream, subject string) (rawEntry, bool, error) {
	var response msgGetResponse
	if err := request(ctx, nc, msgGetSubject+stream, msgGetRequest{LastFor: subject}, &response); err != nil {
		return rawEntry{}, false, err
	}
	if response.Error != nil {
		return noMessage(response.Error)
	}
	if response.Message == nil || response.Message.Subject != subject {
		return rawEntry{}, false, fmt.Errorf("get %s: %w", subject, errMalformedAnswer)
	}
	return entryOf(response.Message)
}

func noMessage(answer *apiError) (rawEntry, bool, error) {
	if jetstream.ErrorCode(answer.ErrCode) == jetstream.JSErrCodeMessageNotFound {
		return rawEntry{}, false, nil
	}
	return rawEntry{}, false, answer.asError()
}

func entryOf(message *storedMessage) (rawEntry, bool, error) {
	entry := rawEntry{
		Sequence: message.Sequence,
		Data:     message.Data,
	}
	if len(message.Header) == 0 {
		return entry, true, nil
	}
	header, err := strictHeaders(message.Header)
	if err != nil {
		return rawEntry{}, false, fmt.Errorf("headers of %s: %w", message.Subject, err)
	}
	entry.Header = header
	return entry, true, nil
}

func strictHeaders(block []byte) (nats.Header, error) {
	lines, err := headerLines(block)
	if err != nil {
		return nil, err
	}
	header := nats.Header{}
	for _, line := range lines {
		key, value, err := headerField(line)
		if err != nil {
			return nil, err
		}
		header[key] = append(header[key], value)
	}
	return header, nil
}

func headerLines(block []byte) ([]string, error) {
	text, terminated := strings.CutSuffix(string(block), headerLineEnd+headerLineEnd)
	lines := strings.Split(text, headerLineEnd)
	if !terminated || lines[0] != headerStatus {
		return nil, errInvalidEntry
	}
	return lines[1:], nil
}

func headerField(line string) (string, string, error) {
	key, value, found := strings.Cut(line, headerSeparator)
	if !found || key == "" || strings.ContainsAny(key, keyBlanks) || strings.ContainsAny(line, bareLineBreaks) {
		return "", "", errInvalidEntry
	}
	return key, strings.TrimSpace(value), nil
}

func purge(ctx context.Context, nc *nats.Conn, stream, subject string) (uint64, error) {
	var response purgeResponse
	if err := request(ctx, nc, purgeSubject+stream, purgeRequest{Filter: subject}, &response); err != nil {
		return 0, err
	}
	if response.Error != nil {
		return 0, response.Error.asError()
	}
	if !response.Success {
		return 0, fmt.Errorf("purge %s: %w", subject, errMalformedAnswer)
	}
	return response.Purged, nil
}

func request(ctx context.Context, nc *nats.Conn, subject string, body, answer any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode %s: %w", subject, err)
	}
	reply, err := nc.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("request %s: %w", subject, err)
	}
	if err := json.Unmarshal(reply.Data, answer); err != nil {
		return fmt.Errorf("answer of %s: %w", subject, errMalformedAnswer)
	}
	return nil
}

func (e *apiError) asError() error {
	return &jetstream.APIError{
		Code:        e.Code,
		ErrorCode:   jetstream.ErrorCode(e.ErrCode),
		Description: e.Description,
	}
}
