//go:build test
// +build test

package isaac

func (vp *baseVoteproof) SetID(id string) {
	vp.id = id
}
