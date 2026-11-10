package response

import "strconv"

const (
	attrRPCResponseStatusCode  = "rpc.response.status_code"
	attrRPCGRPCStatusCode      = "rpc.grpc.status_code"
	attrHTTPResponseStatusCode = "http.response.status_code"
	attrHTTPStatusCode         = "http.status_code"
	attrErrorType              = "error.type"
	attrDBSystemName           = "db.system.name"
	attrDBSystem               = "db.system"
	attrMessagingSystem        = "messaging.system"

	minHTTPServerErrorCode = 500
)

type grpcCodeName string

const (
	grpcCodeNameOK            grpcCodeName = "OK"
	grpcCodeNameCancelled     grpcCodeName = "CANCELLED"
	grpcCodeNameNotFound      grpcCodeName = "NOT_FOUND"
	grpcCodeNameAlreadyExists grpcCodeName = "ALREADY_EXISTS"
)

type grpcCode int

const (
	grpcCodeOK            grpcCode = 0
	grpcCodeCancelled     grpcCode = 1
	grpcCodeNotFound      grpcCode = 5
	grpcCodeAlreadyExists grpcCode = 6
)

type Attributes struct {
	RPCResponseStatusCode  string
	RPCGRPCStatusCode      string
	HTTPResponseStatusCode string
	HTTPStatusCode         string
	ErrorType              string
	DBSystemName           string
	DBSystem               string
	MessagingSystem        string
}

func (a *Attributes) Set(key, value string) bool {
	field := a.field(key)
	if field == nil {
		return false
	}
	*field = value
	return true
}

func (a *Attributes) field(key string) *string {
	switch key {
	case attrRPCResponseStatusCode:
		return &a.RPCResponseStatusCode
	case attrRPCGRPCStatusCode:
		return &a.RPCGRPCStatusCode
	case attrHTTPResponseStatusCode:
		return &a.HTTPResponseStatusCode
	case attrHTTPStatusCode:
		return &a.HTTPStatusCode
	}
	return a.contextField(key)
}

func (a *Attributes) contextField(key string) *string {
	switch key {
	case attrErrorType:
		return &a.ErrorType
	case attrDBSystemName:
		return &a.DBSystemName
	case attrDBSystem:
		return &a.DBSystem
	case attrMessagingSystem:
		return &a.MessagingSystem
	}
	return nil
}

func (a *Attributes) Failed(statusError bool) bool {
	if failed, found := a.rpcFailed(); found {
		return failed
	}
	if failed, found := a.httpFailed(); found {
		return failed
	}
	if a.ErrorType != "" {
		return true
	}
	return statusError
}

func (a *Attributes) rpcFailed() (failed, found bool) {
	if a.RPCResponseStatusCode != "" {
		return !isBenignGRPCCodeName(grpcCodeName(a.RPCResponseStatusCode)), true
	}
	raw := a.RPCGRPCStatusCode
	if raw == "" {
		return false, false
	}
	code, err := strconv.Atoi(raw)
	if err != nil {
		return false, false
	}
	return !isBenignGRPCCode(grpcCode(code)), true
}

func (a *Attributes) httpFailed() (failed, found bool) {
	for _, raw := range [...]string{
		a.HTTPResponseStatusCode,
		a.HTTPStatusCode,
	} {
		if raw == "" {
			continue
		}
		code, err := strconv.Atoi(raw)
		if err == nil {
			return code >= minHTTPServerErrorCode, true
		}
	}
	return false, false
}

func isBenignGRPCCodeName(name grpcCodeName) bool {
	switch name {
	case grpcCodeNameOK, grpcCodeNameCancelled, grpcCodeNameNotFound, grpcCodeNameAlreadyExists:
		return true
	}
	return false
}

func isBenignGRPCCode(code grpcCode) bool {
	switch code {
	case grpcCodeOK, grpcCodeCancelled, grpcCodeNotFound, grpcCodeAlreadyExists:
		return true
	}
	return false
}

func (a *Attributes) CallsExternalSystem() bool {
	return a.DBSystemName != "" || a.DBSystem != "" || a.MessagingSystem != ""
}

func Relevant(key string) bool {
	var probe Attributes
	return probe.field(key) != nil
}
