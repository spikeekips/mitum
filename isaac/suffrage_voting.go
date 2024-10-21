package isaac

import (
	"context"
	"math/bits"
	"reflect"
	"sort"
	"sync"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/exp/slices"
)

type SuffrageVoteFunc func(base.SuffrageExpelOperation) (bool, error)

type SuffrageVoting struct {
	local         base.Address
	db            SuffrageExpelPool
	votedCallback func(base.SuffrageExpelOperation) error
	existsInState func(util.Hash) (bool, error)
	l             sync.Mutex
}

func NewSuffrageVoting(
	local base.Address,
	db SuffrageExpelPool,
	existsInState func(util.Hash) (bool, error),
	votedCallback func(base.SuffrageExpelOperation) error,
) *SuffrageVoting {
	nvotedCallback := votedCallback
	if nvotedCallback == nil {
		nvotedCallback = func(base.SuffrageExpelOperation) error {
			return nil
		}
	}

	return &SuffrageVoting{
		local:         local,
		db:            db,
		existsInState: existsInState,
		votedCallback: nvotedCallback,
	}
}

func (s *SuffrageVoting) Vote(op base.SuffrageExpelOperation) (bool, error) {
	s.l.Lock()
	defer s.l.Unlock()

	e := util.StringError("suffrage voting")

	fact := op.ExpelFact()

	if fact.Node().Equal(s.local) {
		return false, nil
	}

	switch found, err := s.existsInState(fact.Hash()); {
	case err != nil:
		return false, e.Wrap(err)
	case found:
		return false, nil
	}

	var voted base.SuffrageExpelOperation

	switch i, found, err := s.db.SuffrageExpelOperation(fact.ExpelStart(), fact.Node()); {
	case err != nil:
		return false, e.Wrap(err)
	case !found:
		voted, err = s.merge(nil, op)
		if err != nil {
			return false, nil
		}
	default:
		if i.Hash().Equal(op.Hash()) {
			return false, nil
		}

		voted, err = s.merge(i, op)
		if err != nil {
			return false, nil
		}
	}

	if voted != nil {
		_ = s.votedCallback(voted)
	}

	return voted != nil, nil
}

func (s *SuffrageVoting) Find(
	ctx context.Context,
	height base.Height,
	suf base.Suffrage,
) ([]base.SuffrageExpelOperation, error) {
	e := util.StringError("collect suffrage expel operations")

	if h := height.Prev(); h >= base.GenesisHeight {
		// NOTE remove expires
		defer func() {
			_ = s.db.RemoveSuffrageExpelOperationsByHeight(h)
		}()
	}

	var collected []base.SuffrageExpelOperation

	var expires []base.SuffrageExpelFact

	if err := s.db.TraverseSuffrageExpelOperations(
		ctx,
		height,
		func(op base.SuffrageExpelOperation) (bool, error) {
			fact := op.ExpelFact()

			if !suf.Exists(fact.Node()) {
				expires = append(expires, fact)

				return false, nil
			}

			collected = append(collected, op)

			return true, nil
		}); err != nil {
		return nil, e.Wrap(err)
	}

	if len(expires) > 0 {
		// NOTE remove unknowns
		defer func() {
			_ = s.db.RemoveSuffrageExpelOperationsByFact(expires)
		}()
	}

	if len(collected) < 1 {
		return nil, nil
	}

	// NOTE sort by fact hash
	sort.Slice(collected, func(i, j int) bool {
		return collected[i].Hash().String() < collected[j].Hash().String()
	})

	var expelnodes []string
	var found []base.SuffrageExpelOperation

	if j, k := s.findExpelCombinations(collected, suf); len(j) > 0 {
		expelnodes = j
		found = k
	}

	if len(found) < 1 {
		return nil, nil
	}

	ops := make([]base.SuffrageExpelOperation, len(found))

	for i := range found {
		j, err := s.filterSigns(found[i], expelnodes)
		if err != nil {
			return nil, e.Wrap(err)
		}

		ops[i] = j
	}

	return ops, nil
}

func (s *SuffrageVoting) merge(existing, newop base.SuffrageExpelOperation) (base.SuffrageExpelOperation, error) {
	if existing == nil {
		return newop, s.db.SetSuffrageExpelOperation(newop)
	}

	var nodesigner base.NodeSigner
	if err := util.ReflectPtrSetInterfaceValue(existing, &nodesigner); err != nil {
		return nil, err
	}

	switch added, err := nodesigner.AddNodeSigns(newop.NodeSigns()); {
	case err != nil:
		return nil, err
	case !added:
		return nil, nil
	default:
		updated := nodesigner.(base.SuffrageExpelOperation) //nolint:forcetypeassert //...

		if err := s.db.SetSuffrageExpelOperation(updated); err != nil {
			return nil, err
		}

		return updated, nil
	}
}

func (*SuffrageVoting) findExpelCombinations(ops []base.SuffrageExpelOperation, suf base.Suffrage) (
	expelnodes []string,
	found []base.SuffrageExpelOperation,
) {
	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))

	n := len(ops)

	findExpelCombinations(ops, 1, n,
		func(i int, op base.SuffrageExpelOperation) bool {
			signs := op.NodeSigns()

			for j := range signs {
				// NOTE unknown node
				if !suf.Exists(signs[j].Node()) {
					return false
				}
			}

			newthreshold := threshold

			if uint(i) > uint(suf.Len())-threshold {
				newthreshold = uint(suf.Len()) - uint(i)
			}

			return uint(len(signs)) >= newthreshold // NOTE under threshold
		},
		func(ops []base.SuffrageExpelOperation) bool {
			newthreshold := threshold

			if i := uint(len(ops)); i > uint(suf.Len())-threshold {
				newthreshold = uint(suf.Len()) - i
			}

			nodes := make([]string, len(ops))
			for j := range ops {
				nodes[j] = ops[j].ExpelFact().Node().String()
			}

			for j := range ops {
				signs := ops[j].NodeSigns()

				// NOTE check signs except expel nodes
				if k := util.CountFilteredSlice(signs, func(x base.NodeSign) bool {
					return slices.Index(nodes, x.Node().String()) < 0
				}); uint(k) < newthreshold {
					return true
				}
			}

			expelnodes = nodes
			found = ops

			return false
		},
	)

	return expelnodes, found
}

func (*SuffrageVoting) filterSigns(
	op base.SuffrageExpelOperation, expelnodes []string,
) (base.SuffrageExpelOperation, error) {
	signs := op.NodeSigns()

	filtered := util.FilterSlice(signs, func(i base.NodeSign) bool {
		return slices.Index(expelnodes, i.Node().String()) < 0
	})

	if len(signs) == len(filtered) {
		return op, nil
	}

	var nodesigner base.NodeSigner
	if err := util.ReflectPtrSetInterfaceValue(op, &nodesigner); err != nil {
		return nil, err
	}

	if err := nodesigner.SetNodeSigns(filtered); err != nil {
		return nil, err
	}

	return reflect.ValueOf(nodesigner). //nolint:forcetypeassert //...
						Elem().Interface().(base.SuffrageExpelOperation), nil
}

// NOTE findBallotCombinations finds the possible combinations by number of
// signs; it was derived from
// https://github.com/mxschmitt/golang-combinations/blob/v1.1.0/combinations.go#L32
func findExpelCombinations(
	ops []base.SuffrageExpelOperation,
	minv, maxv int,
	filterOperation func(int, base.SuffrageExpelOperation) bool,
	callback func([]base.SuffrageExpelOperation) bool,
) {
	if maxv > len(ops) {
		return
	}

	length := len(ops)

end:
	for i := (1 << length) - 1; i > 0; i-- { // NOTE big combination first
		n := bits.OnesCount(uint(i))

		switch {
		case minv >= 0 && n < minv,
			maxv >= 0 && n > maxv:
			continue
		}

		set := make([]base.SuffrageExpelOperation, maxv)

		var k int
		for j := 0; j < length; j++ {
			if (i>>j)&1 == 1 {
				if !filterOperation(n, ops[j]) {
					continue end
				}

				set[k] = ops[j]

				k++
			}
		}

		// NOTE less signed first for threshold comparison
		sort.Slice(set[:n], func(i, j int) bool {
			return len(set[i].NodeSigns()) < len(set[j].NodeSigns())
		})

		if !callback(set[:n]) {
			break end
		}
	}
}
