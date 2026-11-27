package selfalert

func authRules() []rule {
	return []rule{
		flagRule(AuthDirectoryStale, func(sample *Sample) bool {
			return sample.TokensStale
		}),
		flagRule(AuthSequenceRegressed, func(sample *Sample) bool {
			return sample.TokensRegressed
		}),
		flagRule(AuthRevokeBlocked, func(sample *Sample) bool {
			return sample.RevokeBlocked
		}),
		flagRule(AuthBucketDrift, func(sample *Sample) bool {
			return sample.BucketDrift
		}),
		heldCountRule(AuthEpochChanged, countHold, func(sample *Sample) uint64 {
			return sample.EpochChanges
		}),
		heldCountRule(AuthMassRevoke, countHold, func(sample *Sample) uint64 {
			return sample.MassRevokes
		}),
	}
}
