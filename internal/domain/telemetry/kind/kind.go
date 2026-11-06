package kind

type Kind uint8

const (
	Unspecified Kind = 0
	Internal    Kind = 1
	Server      Kind = 2
	Client      Kind = 3
	Producer    Kind = 4
	Consumer    Kind = 5
)

func (k Kind) Normalized() Kind {
	if k > Consumer {
		return Unspecified
	}
	return k
}
