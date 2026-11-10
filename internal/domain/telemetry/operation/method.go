package operation

type httpMethod string

const (
	methodGet     httpMethod = "GET"
	methodHead    httpMethod = "HEAD"
	methodPost    httpMethod = "POST"
	methodPut     httpMethod = "PUT"
	methodDelete  httpMethod = "DELETE"
	methodConnect httpMethod = "CONNECT"
	methodOptions httpMethod = "OPTIONS"
	methodTrace   httpMethod = "TRACE"
	methodPatch   httpMethod = "PATCH"
	methodOther   httpMethod = "_OTHER"
)

func canonicalMethod(raw string) httpMethod {
	switch method := httpMethod(raw); method {
	case methodGet, methodHead, methodPost, methodPut, methodDelete, methodConnect, methodOptions, methodTrace,
		methodPatch:
		return method
	}
	return methodOther
}

type rpcSystem string

const (
	rpcSystemGRPC    rpcSystem = "grpc"
	rpcSystemConnect rpcSystem = "connect_rpc"
)

func rpcProtocol(raw string) Protocol {
	switch rpcSystem(raw) {
	case rpcSystemGRPC, rpcSystemConnect:
		return ProtocolGRPC
	}
	return ProtocolRPC
}
