package isaacdatabase

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type basePermanent struct {
	encs   *encoder.Encoders
	enc    encoder.Encoder
	lenc   *util.Locked[hint.Hint]          // NOTE encoder of last blockmap
	mp     *util.Locked[[3]interface{}]     // NOTE last blockmap
	policy *util.Locked[base.NetworkPolicy] // NOTE last NetworkPolicy
	proof  *util.Locked[[3]interface{}]     // NOTE last SuffrageProof
}

func newBasePermanent(encs *encoder.Encoders, enc encoder.Encoder) *basePermanent {
	return &basePermanent{
		encs:   encs,
		enc:    enc,
		lenc:   util.EmptyLocked[hint.Hint](),
		mp:     util.EmptyLocked[[3]interface{}](),
		policy: util.EmptyLocked[base.NetworkPolicy](),
		proof:  util.EmptyLocked[[3]interface{}](),
	}
}

func (db *basePermanent) LastBlockMap() (base.BlockMap, bool, error) {
	switch i, isempty := db.mp.Value(); {
	case isempty:
		return nil, false, nil
	default:
		return i[0].(base.BlockMap), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastBlockMapBytes() (enchint hint.Hint, meta, body []byte, found bool, err error) {
	switch i, isempty := db.lenc.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		enchint = i
	}

	switch i, isempty := db.mp.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		return enchint, i[1].([]byte), i[2].([]byte), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProof() (base.SuffrageProof, bool, error) {
	switch i, isempty := db.proof.Value(); {
	case isempty:
		return nil, false, nil
	default:
		return i[0].(base.SuffrageProof), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProofBytes() (enchint hint.Hint, meta, body []byte, found bool, err error) {
	switch i, isempty := db.lenc.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		enchint = i
	}

	switch i, isempty := db.proof.Value(); {
	case isempty:
		return enchint, nil, nil, false, nil
	default:
		return enchint, i[1].([]byte), i[2].([]byte), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastNetworkPolicy() base.NetworkPolicy {
	switch i, _ := db.policy.Value(); {
	case i == nil:
		return nil
	default:
		return i
	}
}

func (db *basePermanent) Clean() error {
	_ = db.mp.Empty(func([3]interface{}, bool) error {
		db.policy.EmptyValue()
		db.proof.EmptyValue()

		return nil
	})

	return nil
}

func (db *basePermanent) updateLast(
	lenc hint.Hint,
	mp base.BlockMap, mpmeta, mpbody []byte,
	proof base.SuffrageProof, proofmeta, proofbody []byte,
	policy base.NetworkPolicy,
) (updated bool) {
	_, err := db.mp.Set(func(i [3]interface{}, isempty bool) ([3]interface{}, error) {
		if !isempty {
			old := i[0].(base.BlockMap) //nolint:forcetypeassert //...

			if mp.Manifest().Height() <= old.Manifest().Height() {
				return [3]interface{}{}, errors.Errorf("old")
			}
		}

		_ = db.lenc.SetValue(lenc)

		if proof != nil {
			_ = db.proof.SetValue([3]interface{}{proof, proofmeta, proofbody})
		}

		if policy != nil {
			_ = db.policy.SetValue(policy)
		}

		return [3]interface{}{mp, mpmeta, mpbody}, nil
	})

	return err == nil
}
