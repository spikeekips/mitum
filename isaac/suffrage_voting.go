package isaac

import (
	"context"
	"math/bits"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
)

type SuffrageVoteFunc func(base.SuffrageWithdrawOperation) (bool, error)

type SuffrageVoting struct {
	local         base.Address
	db            SuffrageWithdrawPool
	votedCallback func(base.SuffrageWithdrawOperation) error
	existsInState func(util.Hash) (bool, error)
}

func NewSuffrageVoting(
	local base.Address,
	db SuffrageWithdrawPool,
	existsInState func(util.Hash) (bool, error),
	votedCallback func(base.SuffrageWithdrawOperation) error,
) *SuffrageVoting {
	if votedCallback == nil {
		votedCallback = func(base.SuffrageWithdrawOperation) error { //revive:disable-line:modifies-parameter
			return nil
		}
	}

	return &SuffrageVoting{
		local:         local,
		db:            db,
		existsInState: existsInState,
		votedCallback: votedCallback,
	}
}

func (s *SuffrageVoting) Vote(op base.SuffrageWithdrawOperation) (bool, error) {
	e := util.StringErrorFunc("failed suffrage voting")

	fact := op.WithdrawFact()

	if fact.Node().Equal(s.local) {
		return false, nil
	}

	switch found, err := s.existsInState(fact.Hash()); {
	case err != nil:
		return false, e(err, "")
	case found:
		return false, nil
	}

	var voted base.SuffrageWithdrawOperation

	switch i, found, err := s.db.SuffrageWithdrawOperation(fact.WithdrawStart(), fact.Node()); {
	case err != nil:
		return false, e(err, "")
	case !found:
		voted, err = s.merge(nil, op)
		if err != nil {
			return false, e(err, "")
		}
	default:
		if i.Hash().Equal(op.Hash()) {
			return false, nil
		}

		voted, err = s.merge(i, op)
		if err != nil {
			return false, e(err, "")
		}
	}

	if voted != nil {
		if err := s.votedCallback(voted); err != nil {
			return true, e(err, "")
		}
	}

	return voted != nil, nil
}

func (s *SuffrageVoting) Find(
	ctx context.Context,
	height base.Height,
	suf base.Suffrage,
) ([]base.SuffrageWithdrawOperation, error) {
	e := util.StringErrorFunc("failed to collect suffrage withdraw operations")

	if h := height.Prev(); h >= base.GenesisHeight {
		// NOTE remove expires
		defer func() {
			_ = s.db.RemoveSuffrageWithdrawOperationsByHeight(h)
		}()
	}

	var collected []base.SuffrageWithdrawOperation

	var expires []base.SuffrageWithdrawFact

	if err := s.db.TraverseSuffrageWithdrawOperations(
		ctx,
		height,
		func(op base.SuffrageWithdrawOperation) (bool, error) {
			fact := op.WithdrawFact()

			if !suf.Exists(fact.Node()) {
				expires = append(expires, fact)

				return false, nil
			}

			collected = append(collected, op)

			return true, nil
		}); err != nil {
		return nil, e(err, "")
	}

	if len(expires) > 0 {
		// NOTE remove unknowns
		defer func() {
			_ = s.db.RemoveSuffrageWithdrawOperationsByFact(expires)
		}()
	}

	if len(collected) < 1 {
		return nil, nil
	}

	// NOTE sort by fact hash
	sort.Slice(collected, func(i, j int) bool {
		return strings.Compare(collected[i].Hash().String(), collected[j].Hash().String()) < 0
	})

	var withdrawnodes []string
	var found []base.SuffrageWithdrawOperation

	if j, k := s.findWithdrawCombinations(collected, suf); len(j) > 0 {
		withdrawnodes = j
		found = k
	}

	if len(found) < 1 {
		return nil, nil
	}

	ops := make([]base.SuffrageWithdrawOperation, len(found))

	for i := range found {
		j, err := s.filterSigns(found[i], withdrawnodes)
		if err != nil {
			return nil, e(err, "")
		}

		ops[i] = j
	}

	return ops, nil
}

func (s *SuffrageVoting) merge(existing, newop base.SuffrageWithdrawOperation) (base.SuffrageWithdrawOperation, error) {
	if existing == nil {
		return newop, s.db.SetSuffrageWithdrawOperation(newop)
	}

	ptr := reflect.New(reflect.ValueOf(existing).Type()).Interface()

	if err := util.InterfaceSetValue(existing, ptr); err != nil {
		return nil, err
	}

	nodesigner, ok := ptr.(base.NodeSigner)
	if !ok {
		return nil, errors.Errorf("expected NodeSigner, but %T", existing)
	}

	switch added, err := nodesigner.AddNodeSigns(newop.NodeSigns()); {
	case err != nil:
		return nil, err
	case !added:
		return nil, nil
	default:
		updated := nodesigner.(base.SuffrageWithdrawOperation) //nolint:forcetypeassert //...

		if err := s.db.SetSuffrageWithdrawOperation(updated); err != nil {
			return nil, err
		}

		return updated, nil
	}
}

func (*SuffrageVoting) findWithdrawCombinations(ops []base.SuffrageWithdrawOperation, suf base.Suffrage) (
	withdrawnodes []string,
	found []base.SuffrageWithdrawOperation,
) {
	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))

	n := len(ops)

	findWithdrawCombinations(ops, 1, n,
		func(i int, op base.SuffrageWithdrawOperation) bool {
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
		func(ops []base.SuffrageWithdrawOperation) bool {
			newthreshold := threshold

			if i := uint(len(ops)); i > uint(suf.Len())-threshold {
				newthreshold = uint(suf.Len()) - i
			}

			nodes := make([]string, len(ops))
			for j := range ops {
				nodes[j] = ops[j].WithdrawFact().Node().String()
			}

			for j := range ops {
				signs := ops[j].NodeSigns()

				// NOTE check signs except withdraw nodes
				if k := util.CountFilteredSlice(signs, func(x base.NodeSign) bool {
					return util.InSlice(nodes, x.Node().String()) < 0
				}); uint(k) < newthreshold {
					return true
				}
			}

			withdrawnodes = nodes
			found = ops

			return false
		},
	)

	return withdrawnodes, found
}

func (*SuffrageVoting) filterSigns(
	op base.SuffrageWithdrawOperation, withdrawnodes []string,
) (base.SuffrageWithdrawOperation, error) {
	signs := op.NodeSigns()

	filtered := util.FilterSlice(signs, func(i base.NodeSign) bool {
		return util.InSlice(withdrawnodes, i.Node().String()) < 0
	})

	if len(signs) == len(filtered) {
		return op, nil
	}

	ptr := reflect.New(reflect.ValueOf(op).Type()).Interface()

	if err := util.InterfaceSetValue(op, ptr); err != nil {
		return nil, err
	}

	nodesigner, ok := ptr.(base.NodeSigner)
	if !ok {
		return nil, errors.Errorf("expected NodeSigner, but %T", ptr)
	}

	if err := nodesigner.SetNodeSigns(filtered); err != nil {
		return nil, err
	}

	return reflect.ValueOf(nodesigner). //nolint:forcetypeassert //...
						Elem().Interface().(base.SuffrageWithdrawOperation), nil
}

// NOTE findBallotCombinations finds the possible combinations by number of
// signs; it was derived from
// https://github.com/mxschmitt/golang-combinations/blob/v1.1.0/combinations.go#L32
func findWithdrawCombinations(
	ops []base.SuffrageWithdrawOperation,
	min, max int,
	filterOperation func(int, base.SuffrageWithdrawOperation) bool,
	callback func([]base.SuffrageWithdrawOperation) bool,
) {
	if max > len(ops) {
		return
	}

	length := len(ops)

end:
	for i := (1 << length) - 1; i > 0; i-- { // NOTE big combination first
		n := bits.OnesCount(uint(i))

		switch {
		case min >= 0 && n < min,
			max >= 0 && n > max:
			continue
		}

		set := make([]base.SuffrageWithdrawOperation, max)

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
