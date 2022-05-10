package base

import (
	"sort"
)

type VoteResult string

const (
	VoteResultNotYet   = VoteResult("NOT YET")
	VoteResultDraw     = VoteResult("DRAW")
	VoteResultMajority = VoteResult("MAJORITY")
)

func (v VoteResult) Bytes() []byte {
	return []byte(v)
}

func (v VoteResult) String() string {
	switch v {
	case VoteResultNotYet, VoteResultDraw, VoteResultMajority:
		return string(v)
	default:
		return "NOT YET"
	}
}

func (VoteResult) IsValid([]byte) error {
	return nil
}

func (v VoteResult) MarshalText() ([]byte, error) {
	return []byte(v.String()), nil
}

func (v *VoteResult) UnmarshalText(b []byte) error {
	i := VoteResult(string(b))

	switch i {
	case VoteResultNotYet, VoteResultDraw, VoteResultMajority:
	default:
		i = VoteResultNotYet
	}

	*v = i

	return nil
}

// FindMajority finds the majority(over threshold) set between the given sets.
// The returned value means,
// 0-N: index number of set
// -1: not yet majority
// -2: draw
func FindMajority(quorum, threshold uint, set ...uint) int {
	th := threshold
	if th > quorum {
		th = quorum
	}

	if len(set) < 1 {
		return -1
	}

	var sum uint

	for i := range set {
		n := set[i]

		if n >= quorum {
			return i
		}

		if n >= th {
			return i
		}

		sum += n
	}

	sort.Slice(set, func(i, j int) bool {
		return set[i] > set[j]
	})

	if quorum-sum+set[0] < th {
		return -2
	}

	return -1
}

func FindVoteResult(quorum, threshold uint, s []string) (result VoteResult, key string) {
	th := threshold
	if th > quorum {
		th = quorum
	}

	switch {
	case len(s) < 1:
		return VoteResultNotYet, ""
	case uint(len(s)) < th:
		return VoteResultNotYet, ""
	}

	keys := map[uint]string{}
	count := map[string]uint{}

	for i := range s {
		count[s[i]]++
	}

	set := make([]uint, len(count))
	var i int

	for j := range count {
		c := count[j]
		keys[c] = j
		set[i] = c
		i++
	}

	sort.Slice(set, func(i, j int) bool { return set[i] > set[j] })

	switch index := FindMajority(quorum, th, set...); index {
	case -1:
		return VoteResultNotYet, ""
	case -2:
		return VoteResultDraw, ""
	default:
		return VoteResultMajority, keys[set[index]]
	}
}
