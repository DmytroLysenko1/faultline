package main

import "os"

func main() {
	os.Exit(realMain(process{
		Args:   os.Args[1:],
		Lookup: os.LookupEnv,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}))
}
