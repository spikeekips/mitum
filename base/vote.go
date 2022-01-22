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
func FindMajority(total, threshold uint, set ...uint) int {
	if threshold > total {
		threshold = total
	}

	if len(set) < 1 {
		return -1
	}

	var sum uint
	for i := range set {
		n := set[i]

		if n >= total {
			return i
		}

		// check majority
		if n >= threshold {
			return i
		}

		sum += n
	}

	sort.Slice(set, func(i, j int) bool {
		return set[i] > set[j]
	})

	if total-sum+set[0] < threshold {
		return -2 // draw
	}

	return -1 // not yet
}

func FindVoteResult(total, threshold uint, s []string) (VoteResult, string) {
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
	switch index := FindMajority(total, threshold, set...); index {
	case -1:
		return VoteResultNotYet, ""
	case -2:
		return VoteResultDraw, ""
	default:
		return VoteResultMajority, keys[set[index]]
	}
}
