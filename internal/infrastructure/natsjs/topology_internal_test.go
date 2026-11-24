package natsjs

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	pinnedVersion  = 1
	pinnedHash     = "0268c631f05be163e8e683fe70514c37bf30212cd3923fc81d8f4326f6bce87d"
	pinnedMaxBytes = 2 << 30
	pinnedWindow   = 5 * time.Minute
	pinnedReplicas = 3
)

type configPin struct {
	Version int
	Hash    string
}

func TestTheStreamConfigIsPinnedToItsVersion(t *testing.T) {
	topology := Topology{
		Partitions:           1,
		Replicas:             pinnedReplicas,
		MaxBytesPerPartition: pinnedMaxBytes,
		DuplicateWindow:      pinnedWindow,
		Compression:          CompressionS2,
		ConfigVersion:        StreamConfigVersion,
	}
	rendered := renderStreams(topology.config(0), topology.deadLetterConfig())
	sum := sha256.Sum256([]byte(rendered))

	got := configPin{
		Version: StreamConfigVersion,
		Hash:    hex.EncodeToString(sum[:]),
	}

	want := configPin{
		Version: pinnedVersion,
		Hash:    pinnedHash,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("the streams natsjs builds changed without a new StreamConfigVersion, or the version moved "+
			"without the streams: bump StreamConfigVersion together with this pin; a new field in the table moves the "+
			"pin, a nats.go upgrade does not (-want +got):\n%s\nbuilt:\n%s",
			diff, rendered)
	}
}

func renderStreams(configs ...jetstream.StreamConfig) string {
	var lines []string
	for index := range configs {
		lines = append(lines, "name="+configs[index].Name)
		for _, compared := range fields {
			lines = append(lines, compared.name+"="+compared.value(&configs[index]))
		}
	}
	return strings.Join(lines, "\n")
}
