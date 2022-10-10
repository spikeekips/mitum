package isaac

import (
	"context"
	"reflect"

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

func (s *SuffrageVoting) Collect(ctx context.Context, height base.Height, suf base.Suffrage) ([]base.SuffrageWithdrawOperation, error) {
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
