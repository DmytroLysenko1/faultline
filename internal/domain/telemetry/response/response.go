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

type Attributes map[string]string

func (a Attributes) Failed(statusError bool) bool {
	if failed, found := a.rpcFailed(); found {
		return failed
	}
	if failed, found := a.httpFailed(); found {
		return failed
	}
	if a[attrErrorType] != "" {
		return true
	}
	return statusError
}

func (a Attributes) rpcFailed() (failed, found bool) {
	if name := a[attrRPCResponseStatusCode]; name != "" {
		return !isBenignGRPCCodeName(grpcCodeName(name)), true
	}
	raw := a[attrRPCGRPCStatusCode]
	if raw == "" {
		return false, false
	}
	code, err := strconv.Atoi(raw)
	if err != nil {
		return false, false
	}
	return !isBenignGRPCCode(grpcCode(code)), true
}

func (a Attributes) httpFailed() (failed, found bool) {
	for _, key := range [...]string{
		attrHTTPResponseStatusCode,
		attrHTTPStatusCode,
	} {
		raw := a[key]
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

func (a Attributes) CallsExternalSystem() bool {
	return a[attrDBSystemName] != "" ||
		a[attrDBSystem] != "" ||
		a[attrMessagingSystem] != ""
}
