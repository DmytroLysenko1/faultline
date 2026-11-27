package main

import (
	"os"
	"runtime/debug"
)

func main() {
	os.Exit(realMain(&process{
		Args:           os.Args[1:],
		Lookup:         os.LookupEnv,
		ReadCgroup:     readCgroupMemoryMax,
		SetMemoryLimit: debug.SetMemoryLimit,
		Stdout:         os.Stdout,
		Stderr:         os.Stderr,
		Hostname:       os.Hostname,
	}))
}
