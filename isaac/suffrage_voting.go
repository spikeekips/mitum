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

type SuffrageVoting struct {
	local         base.Address
	db            SuffrageWithdrawPool
	votedCallback func(base.SuffrageWithdrawOperation) error
}

func NewSuffrageVoting(
	local base.Address,
	db SuffrageWithdrawPool,
	votedCallback func(base.SuffrageWithdrawOperation) error,
) *SuffrageVoting {
	return &SuffrageVoting{
		local:         local,
		db:            db,
		votedCallback: votedCallback,
	}
}

// FIXME before Vote(), op should be passed by IsValidWithdrawWithSuffrage and
// IsValidWithdrawWithSuffrageLifespan.
// FIXME if local is not in consensus nodes, don't Vote().

func (s *SuffrageVoting) Vote(op base.SuffrageWithdrawOperation) (bool, error) {
	e := util.StringErrorFunc("failed suffrage voting")

	fact := op.WithdrawFact()

	if fact.Node().Equal(s.local) {
		return false, nil
	}

	var voted base.SuffrageWithdrawOperation

	switch i, found, err := s.db.SuffrageWithdrawOperation(fact.WithdrawStart(), fact.Node()); {
	case err != nil:
		return false, e(err, "")
	case found:
		voted, err = s.merge(op, i)
		if err != nil {
			return false, e(err, "")
		}
	default:
		voted, err = s.merge(op, nil)
		if err != nil {
			return false, e(err, "")
		}
	}

	if voted != nil {
		if err := s.votedCallback(voted); err != nil {
			return true, e(err, "")
		}
	}

	return true, nil
}

func (s *SuffrageVoting) Find(
	ctx context.Context,
	height base.Height,
	suf base.Suffrage,
) ([]base.SuffrageWithdrawOperation, error) {
	e := util.StringErrorFunc("failed to collect suffrage withdraw operations")

	if h := height.Prev(); h >= base.GenesisHeight {
		defer func() {
			_ = s.db.RemoveSuffrageWithdrawOperationsByHeight(h)
		}()
	}

	var collected []base.SuffrageWithdrawOperation

	// FIXME remove useless

	var expires []util.Hash

	if err := s.db.TraverseSuffrageWithdrawOperations(
		ctx,
		height,
		func(op base.SuffrageWithdrawOperation) (bool, error) {
			fact := op.WithdrawFact()

			if !suf.Exists(fact.Node()) {
				expires = append(expires, fact.Hash())

				return false, nil
			}

			collected = append(collected, op)

			return true, nil
		}); err != nil {
		return nil, e(err, "")
	}

	if len(expires) > 0 {
		defer func() {
			_ = s.db.RemoveSuffrageWithdrawOperationsByHash(expires)
		}()
	}

	if len(collected) < 1 {
		return nil, nil
	}

	// NOTE sort by fact hash
	sort.Slice(collected, func(i, j int) bool {
		return strings.Compare(collected[i].Hash().String(), collected[j].Hash().String()) < 0
	})

	threshold := base.DefaultThreshold.Threshold(uint(suf.Len()))

	for i := len(collected); i > 0; i-- {
		if j := s.findWithdrawCombinations(collected, suf, i, threshold); len(j) > 0 {
			return j, nil
		}
	}

	return nil, nil
}

func (s *SuffrageVoting) merge(op, existing base.SuffrageWithdrawOperation) (base.SuffrageWithdrawOperation, error) {
	if existing == nil {
		return nil, s.db.SetSuffrageWithdrawOperation(op)
	}

	elem := reflect.ValueOf(existing)
	ptr := reflect.New(elem.Type()).Interface()

	if err := util.InterfaceSetValue(elem, ptr); err != nil {
		return nil, err
	}

	nodesigner, ok := ptr.(base.NodeSigner)
	if !ok {
		return nil, errors.Errorf("expected NodeSigner, but %T", existing)
	}

	switch added, err := nodesigner.AddNodeSigns(op.NodeSigns()); {
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

func (*SuffrageVoting) findWithdrawCombinations(
	set []base.SuffrageWithdrawOperation,
	suf base.Suffrage,
	n int,
	threshold uint,
) (found []base.SuffrageWithdrawOperation) {
	newthreshold := threshold

	if i := uint(n); i > uint(suf.Len())-threshold {
		newthreshold = uint(suf.Len()) - i
	}

	findWithdrawCombinations(set, n, n,
		func(op base.SuffrageWithdrawOperation) bool {
			signs := op.NodeSigns()

			for j := range signs {
				// NOTE unknown node
				if !suf.Exists(signs[j].Node()) {
					return false
				}
			}

			return uint(len(signs)) >= newthreshold // NOTE under threshold
		},
		func(ops []base.SuffrageWithdrawOperation) bool {
			// NOTE check valid signs
			withdrawnodes := make([]string, len(ops))
			for j := range ops {
				withdrawnodes[j] = ops[j].WithdrawFact().Node().String()
			}

			for j := range ops {
				signs := ops[j].NodeSigns()

				// NOTE check signs except withdraw nodes
				if k := util.CountFilteredSlice(signs, func(_ interface{}, x int) bool {
					return util.InSlice(withdrawnodes, signs[x].Node().String()) < 0
				}); uint(k) < newthreshold {
					return false
				}
			}

			found = ops

			return false
		},
	)

	return found
}

// NOTE findBallotCombinations finds the possible combinations by number of
// signs; it was derived from
// https://github.com/mxschmitt/golang-combinations/blob/v1.1.0/combinations.go#L32
func findWithdrawCombinations(
	set []base.SuffrageWithdrawOperation,
	min, max int,
	filterOperation func(base.SuffrageWithdrawOperation) bool,
	callback func([]base.SuffrageWithdrawOperation) bool,
) {
	if max > len(set) {
		return
	}

	length := uint(len(set))

end:
	for i := 1; i < (1 << length); i++ {
		switch {
		case min >= 0 && bits.OnesCount(uint(i)) < min,
			max >= 0 && bits.OnesCount(uint(i)) > max:
			continue
		}

		subset := make([]base.SuffrageWithdrawOperation, max)

		var n int

		for j := uint(0); j < length; j++ {
			if (i>>j)&1 == 1 {
				if !filterOperation(set[j]) {
					continue end
				}

				subset[n] = set[j]
				n++
			}
		}

		// NOTE less signed first for threshold comparison
		sort.Slice(subset, func(i, j int) bool {
			return len(subset[i].NodeSigns()) < len(subset[j].NodeSigns())
		})

		if !callback(subset) {
			break end
		}
	}
}
