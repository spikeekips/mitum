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
	lenc   *util.Locked // NOTE last blockmap
	mp     *util.Locked // NOTE last blockmap
	policy *util.Locked // NOTE last NetworkPolicy
	proof  *util.Locked // NOTE last SuffrageProof
}

func newBasePermanent(encs *encoder.Encoders, enc encoder.Encoder) *basePermanent {
	return &basePermanent{
		encs:   encs,
		enc:    enc,
		lenc:   util.EmptyLocked(),
		mp:     util.EmptyLocked(),
		policy: util.EmptyLocked(),
		proof:  util.EmptyLocked(),
	}
}

func (db *basePermanent) LastBlockMap() (base.BlockMap, bool, error) {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		j := i.([3]interface{})                //nolint:forcetypeassert //...
		return j[0].(base.BlockMap), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastBlockMapBytes() (enchint hint.Hint, meta, body []byte, found bool, err error) {
	switch i, _ := db.lenc.Value(); {
	case i == nil:
		return enchint, nil, nil, false, nil
	default:
		enchint = i.(hint.Hint) //nolint:forcetypeassert //...
	}

	switch i, _ := db.mp.Value(); {
	case i == nil:
		return enchint, nil, nil, false, nil
	default:
		j := i.([3]interface{})                                 //nolint:forcetypeassert //...
		return enchint, j[1].([]byte), j[2].([]byte), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProof() (base.SuffrageProof, bool, error) {
	switch i, _ := db.proof.Value(); {
	case i == nil:
		return nil, false, nil
	default:
		j := i.([3]interface{})                     //nolint:forcetypeassert //...
		return j[0].(base.SuffrageProof), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastSuffrageProofBytes() (enchint hint.Hint, meta, body []byte, found bool, err error) {
	switch i, _ := db.lenc.Value(); {
	case i == nil:
		return enchint, nil, nil, false, nil
	default:
		enchint = i.(hint.Hint) //nolint:forcetypeassert //...
	}

	switch i, _ := db.proof.Value(); {
	case i == nil:
		return enchint, nil, nil, false, nil
	default:
		j := i.([3]interface{})                                 //nolint:forcetypeassert //...
		return enchint, j[1].([]byte), j[2].([]byte), true, nil //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) LastNetworkPolicy() base.NetworkPolicy {
	switch i, _ := db.policy.Value(); {
	case i == nil:
		return nil
	default:
		return i.(base.NetworkPolicy) //nolint:forcetypeassert //...
	}
}

func (db *basePermanent) Clean() error {
	_, _ = db.mp.Set(func(bool, interface{}) (interface{}, error) {
		db.policy.SetValue(util.NilLockedValue{})
		db.proof.SetValue(util.NilLockedValue{})

		return util.NilLockedValue{}, nil
	})

	return nil
}

func (db *basePermanent) updateLast(
	lenc hint.Hint,
	mp base.BlockMap, mpmeta, mpbody []byte,
	proof base.SuffrageProof, proofmeta, proofbody []byte,
	policy base.NetworkPolicy,
) (updated bool) {
	_, err := db.mp.Set(func(_ bool, i interface{}) (interface{}, error) {
		if i != nil {
			old := i.(base.BlockMap) //nolint:forcetypeassert //...

			if mp.Manifest().Height() <= old.Manifest().Height() {
				return nil, errors.Errorf("old")
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
