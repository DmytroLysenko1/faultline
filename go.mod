module github.com/DmytroLysenko1/faultline

go 1.27.0

toolchain go1.27.3

tool (
	connectrpc.com/connect/cmd/protoc-gen-connect-go
	github.com/cilium/ebpf/cmd/bpf2go
	golang.org/x/vuln/cmd/govulncheck
	google.golang.org/protobuf/cmd/protoc-gen-go
)

require (
	connectrpc.com/connect v1.21.1 // indirect
	github.com/cilium/ebpf v0.22.0 // indirect
	golang.org/x/mod v0.42.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/telemetry v0.0.0-20261010193011-f71dc92da45e // indirect
	golang.org/x/tools v0.52.0 // indirect
	golang.org/x/vuln v1.9.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
