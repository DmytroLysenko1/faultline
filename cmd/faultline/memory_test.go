package main

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var errNoCgroup = errors.New("no cgroup file")

func cgroupFile(content string) cgroupReader {
	return func() ([]byte, error) {
		return []byte(content), nil
	}
}

func noCgroup() ([]byte, error) {
	return nil, errNoCgroup
}

func TestMemoryLimitPicksItsSource(t *testing.T) {
	type args struct {
		env    map[string]string
		cgroup cgroupReader
	}
	tests := []struct {
		Name    string
		Args    args
		Want    memorySetting
		WantErr error
	}{
		{
			Name: "GOMEMLIMIT in MiB",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "1408MiB",
				},
				cgroup: noCgroup,
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceEnv,
			},
		},
		{
			Name: "GOMEMLIMIT in plain bytes",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "1073741824",
				},
				cgroup: noCgroup,
			},
			Want: memorySetting{
				Bytes:  1 << 30,
				Source: sourceEnv,
			},
		},
		{
			Name: "GOMEMLIMIT in TiB",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "1TiB",
				},
				cgroup: noCgroup,
			},
			Want: memorySetting{
				Bytes:  1 << 40,
				Source: sourceEnv,
			},
		},
		{
			Name: "unset inside a 2 GiB container takes the manifest share",
			Args: args{
				env:    map[string]string{},
				cgroup: cgroupFile("2147483648\n"),
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceCgroup,
			},
		},
		{
			Name: "unset inside a 1 GiB container scales down",
			Args: args{
				env:    map[string]string{},
				cgroup: cgroupFile("1073741824\n"),
			},
			Want: memorySetting{
				Bytes:  704 << 20,
				Source: sourceCgroup,
			},
		},
		{
			Name: "unset with an unlimited cgroup falls back to the default",
			Args: args{
				env:    map[string]string{},
				cgroup: cgroupFile("max\n"),
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceDefault,
			},
		},
		{
			Name: "unset without a cgroup file falls back to the default",
			Args: args{
				env:    map[string]string{},
				cgroup: noCgroup,
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceDefault,
			},
		},
		{
			Name: "GOMEMLIMIT in GiB",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "2GiB",
				},
				cgroup: noCgroup,
			},
			Want: memorySetting{
				Bytes:  2 << 30,
				Source: sourceEnv,
			},
		},
		{
			Name: "GOMEMLIMIT in KiB",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "1441792KiB",
				},
				cgroup: noCgroup,
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceEnv,
			},
		},
		{
			Name: "GOMEMLIMIT equal to the container limit is allowed",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "2147483648B",
				},
				cgroup: cgroupFile("2147483648\n"),
			},
			Want: memorySetting{
				Bytes:  2 << 30,
				Source: sourceEnv,
			},
		},
		{
			Name: "unparsable cgroup file falls back to the default",
			Args: args{
				env:    map[string]string{},
				cgroup: cgroupFile("garbage\n"),
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceDefault,
			},
		},
		{
			Name: "zero cgroup limit falls back to the default",
			Args: args{
				env:    map[string]string{},
				cgroup: cgroupFile("0\n"),
			},
			Want: memorySetting{
				Bytes:  1408 << 20,
				Source: sourceDefault,
			},
		},
		{
			Name: "huge cgroup limit does not overflow",
			Args: args{
				env:    map[string]string{},
				cgroup: cgroupFile("9223372036854771712\n"),
			},
			Want: memorySetting{
				Bytes:  9223372036854771712 / 2048 * 1408,
				Source: sourceCgroup,
			},
		},
		{
			Name: "off disables the budget and is refused",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "off",
				},
				cgroup: noCgroup,
			},
			WantErr: errMemoryLimitOff,
		},
		{
			Name: "unparsable value is refused",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "lots",
				},
				cgroup: noCgroup,
			},
			WantErr: errMemoryLimitInvalid,
		},
		{
			Name: "limit above the container would be OOMKilled before GC reacts",
			Args: args{
				env: map[string]string{
					envMemoryLimit: "3GiB",
				},
				cgroup: cgroupFile("2147483648\n"),
			},
			WantErr: errMemoryLimitContainer,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			got, err := memoryLimit(environment(tt.Args.env), tt.Args.cgroup)
			if !errors.Is(err, tt.WantErr) {
				t.Fatalf("memoryLimit = %v, want %v", err, tt.WantErr)
			}
			if diff := cmp.Diff(tt.Want, got); diff != "" {
				t.Errorf("limit (-want +got):\n%s", diff)
			}
		})
	}
}
