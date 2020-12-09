package config

import (
	"github.com/spikeekips/mitum/base"
)

var defaultCacheSize int = 10

type Suffrage interface {
	SuffrageType() string
}

type FixedSuffrage struct {
	Proposer  base.Address
	Nodes     []base.Address
	CacheSize int
}

func NewFixedSuffrage(proposer base.Address, nodes []base.Address) FixedSuffrage {
	return FixedSuffrage{Proposer: proposer, Nodes: nodes, CacheSize: defaultCacheSize}
}

func (fd FixedSuffrage) SuffrageType() string {
	return "fixed-suffrage"
}

type RoundrobinSuffrage struct {
	NumberOfActing uint
	CacheSize      int
}

func NewRoundrobinSuffrage(numberOfActing uint) RoundrobinSuffrage {
	return RoundrobinSuffrage{CacheSize: defaultCacheSize, NumberOfActing: numberOfActing}
}

func (fd RoundrobinSuffrage) SuffrageType() string {
	return "roundrobin"
}