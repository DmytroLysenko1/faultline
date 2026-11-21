package tokens

import (
	"reflect"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	Bucket          = "faultline_ingest_tokens"
	MaxGrants       = 1024
	MaxValueBytes   = 1 << 10
	MaxMessageBytes = 2 << 10
	MaxBucketBytes  = 1 << 20
	MinDuplicates   = 2 * time.Minute
	keysPerSubject  = 1
)

const (
	streamName    = "KV_" + Bucket
	subjectPrefix = "$KV." + Bucket + "."
	allSubjects   = subjectPrefix + ">"
)

type fieldClass uint8

const (
	classBlocksReading fieldClass = iota
	classStrict
	classWarn
	classIgnored
	classIrreversible
	classCount
)

type configField struct {
	name    string
	class   fieldClass
	differs func(desired, actual *jetstream.StreamConfig) bool
}

type configVerdict struct {
	byClass [classCount][]string
}

func (v *configVerdict) blocksReading() []string {
	return v.byClass[classBlocksReading]
}

func (v *configVerdict) drifted() bool {
	return len(v.byClass[classBlocksReading]) > 0 || len(v.byClass[classStrict]) > 0
}

func (v *configVerdict) irreversible() bool {
	return len(v.byClass[classIrreversible]) > 0
}

func (v *configVerdict) notes() []string {
	return slices.Concat(
		v.byClass[classBlocksReading],
		v.byClass[classStrict],
		v.byClass[classWarn],
		v.byClass[classIrreversible],
	)
}

func desiredConfig(replicas int) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          []string{allSubjects},
		Retention:         jetstream.LimitsPolicy,
		MaxMsgs:           MaxGrants,
		MaxBytes:          MaxBucketBytes,
		Discard:           jetstream.DiscardNew,
		MaxMsgsPerSubject: keysPerSubject,
		MaxMsgSize:        MaxMessageBytes,
		Storage:           jetstream.FileStorage,
		Replicas:          replicas,
		Duplicates:        MinDuplicates,
		DenyDelete:        true,
	}
}

func checkConfig(desired, actual *jetstream.StreamConfig) configVerdict {
	var verdict configVerdict
	for _, field := range configFields {
		if field.differs(desired, actual) {
			verdict.byClass[field.class] = append(verdict.byClass[field.class], field.name)
		}
	}
	return verdict
}

type configValue func(*jetstream.StreamConfig) any

func same[T comparable](name string, value func(*jetstream.StreamConfig) T) configField {
	return configField{
		name: name,
		differs: func(desired, actual *jetstream.StreamConfig) bool {
			return value(desired) != value(actual)
		},
	}
}

func deep(name string, value configValue) configField {
	return configField{
		name: name,
		differs: func(desired, actual *jetstream.StreamConfig) bool {
			return !reflect.DeepEqual(value(desired), value(actual))
		},
	}
}

func set(name string, value func(*jetstream.StreamConfig) bool) configField {
	return configField{
		name: name,
		differs: func(_, actual *jetstream.StreamConfig) bool {
			return value(actual)
		},
	}
}

func shortDuplicateWindow() configField {
	return configField{
		name: "Duplicates",
		differs: func(_, actual *jetstream.StreamConfig) bool {
			return actual.Duplicates < MinDuplicates
		},
	}
}

func ignored(names ...string) []configField {
	fields := make([]configField, 0, len(names))
	for _, name := range names {
		fields = append(fields, configField{
			name: name,
			differs: func(*jetstream.StreamConfig, *jetstream.StreamConfig) bool {
				return false
			},
		})
	}
	return fields
}

func classed(class fieldClass, fields []configField) []configField {
	result := slices.Clone(fields)
	for i := range result {
		result[i].class = class
	}
	return result
}

var grantShapingFields = []configField{
	deep("Subjects", func(c *jetstream.StreamConfig) any { return c.Subjects }),
	deep("Sources", func(c *jetstream.StreamConfig) any { return c.Sources }),
	deep("Mirror", func(c *jetstream.StreamConfig) any { return c.Mirror }),
	deep("SubjectTransform", func(c *jetstream.StreamConfig) any { return c.SubjectTransform }),
	same("MaxMsgsPerSubject", func(c *jetstream.StreamConfig) int64 { return c.MaxMsgsPerSubject }),
	same("Retention", func(c *jetstream.StreamConfig) jetstream.RetentionPolicy { return c.Retention }),
	same("MaxAge", func(c *jetstream.StreamConfig) time.Duration { return c.MaxAge }),
	same("AllowMsgTTL", func(c *jetstream.StreamConfig) bool { return c.AllowMsgTTL }),
	same("AllowMsgCounter", func(c *jetstream.StreamConfig) bool { return c.AllowMsgCounter }),
	same("AllowMsgSchedules", func(c *jetstream.StreamConfig) bool { return c.AllowMsgSchedules }),
	same("MaxMsgs", func(c *jetstream.StreamConfig) int64 { return c.MaxMsgs }),
	same("MaxBytes", func(c *jetstream.StreamConfig) int64 { return c.MaxBytes }),
	same("MaxMsgSize", func(c *jetstream.StreamConfig) int32 { return c.MaxMsgSize }),
}

var writeContractFields = []configField{
	same("NoAck", func(c *jetstream.StreamConfig) bool { return c.NoAck }),
	same("PersistMode", func(c *jetstream.StreamConfig) jetstream.PersistModeType { return c.PersistMode }),
	deep("RePublish", func(c *jetstream.StreamConfig) any { return c.RePublish }),
	same("AllowAtomicPublish", func(c *jetstream.StreamConfig) bool { return c.AllowAtomicPublish }),
	same("AllowBatchPublish", func(c *jetstream.StreamConfig) bool { return c.AllowBatchPublish }),
	same("SubjectDeleteMarkerTTL", func(c *jetstream.StreamConfig) time.Duration { return c.SubjectDeleteMarkerTTL }),
	same("DiscardNewPerSubject", func(c *jetstream.StreamConfig) bool { return c.DiscardNewPerSubject }),
	same("FirstSeq", func(c *jetstream.StreamConfig) uint64 { return c.FirstSeq }),
	same("MirrorDirect", func(c *jetstream.StreamConfig) bool { return c.MirrorDirect }),
	shortDuplicateWindow(),
	same("Discard", func(c *jetstream.StreamConfig) jetstream.DiscardPolicy { return c.Discard }),
	same("AllowRollup", func(c *jetstream.StreamConfig) bool { return c.AllowRollup }),
}

var placementFields = []configField{
	same("Replicas", func(c *jetstream.StreamConfig) int { return c.Replicas }),
	deep("Placement", func(c *jetstream.StreamConfig) any { return c.Placement }),
	same("Compression", func(c *jetstream.StreamConfig) jetstream.StoreCompression { return c.Compression }),
	same("Storage", func(c *jetstream.StreamConfig) jetstream.StorageType { return c.Storage }),
	same("AllowDirect", func(c *jetstream.StreamConfig) bool { return c.AllowDirect }),
	same("DenyDelete", func(c *jetstream.StreamConfig) bool { return c.DenyDelete }),
}

var irreversibleFields = []configField{
	set("DenyPurge", func(c *jetstream.StreamConfig) bool { return c.DenyPurge }),
	set("Sealed", func(c *jetstream.StreamConfig) bool { return c.Sealed }),
}

var serverFilledFields = ignored("Name", "Description", "MaxConsumers", "ConsumerLimits", "Metadata", "Template")

var configFields = slices.Concat(
	classed(classBlocksReading, grantShapingFields),
	classed(classStrict, writeContractFields),
	classed(classWarn, placementFields),
	classed(classIrreversible, irreversibleFields),
	classed(classIgnored, serverFilledFields),
)
