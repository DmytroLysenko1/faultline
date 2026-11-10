package operation

const (
	attrHTTPRequestMethod = "http.request.method"
	attrHTTPMethod        = "http.method"
	attrHTTPRoute         = "http.route"
	attrURLPath           = "url.path"
	attrHTTPTarget        = "http.target"
	attrURLFull           = "url.full"
	attrHTTPURL           = "http.url"
	attrRPCSystemName     = "rpc.system.name"
	attrRPCSystem         = "rpc.system"
	attrRPCService        = "rpc.service"
	attrRPCMethod         = "rpc.method"
)

func (f *Facts) Set(key, value string) bool {
	if field := f.currentField(key); field != nil {
		fill(field, value, value != "")
		return true
	}
	field := f.deprecatedField(key)
	if field == nil {
		return false
	}
	fill(field, value, *field == "")
	return true
}

func fill(field *string, value string, allowed bool) {
	if allowed {
		*field = value
	}
}

func (f *Facts) currentField(key string) *string {
	switch key {
	case attrHTTPRequestMethod:
		return &f.HTTPMethod
	case attrHTTPRoute:
		return &f.Route
	case attrURLPath:
		return &f.Path
	case attrHTTPTarget:
		return &f.Target
	}
	return f.currentRemoteField(key)
}

func (f *Facts) currentRemoteField(key string) *string {
	switch key {
	case attrURLFull:
		return &f.URL
	case attrRPCSystemName:
		return &f.RPCSystem
	case attrRPCService:
		return &f.RPCService
	case attrRPCMethod:
		return &f.RPCMethod
	}
	return nil
}

func (f *Facts) deprecatedField(key string) *string {
	switch key {
	case attrHTTPMethod:
		return &f.HTTPMethod
	case attrHTTPURL:
		return &f.URL
	case attrRPCSystem:
		return &f.RPCSystem
	}
	return nil
}
