package dop5

import (
	"time"

	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/localtime"
)

func DecodeJSONManifest(enc *jsonenc.Encoder, m map[string]any) (any, error) {
	var h, previous, proposal, opsTree, statesTree, suffrage util.Hash
	var height int64
	var proposedAt time.Time

	if err := dutil.FuncChain(
		dutil.AssignJSONHash(m["hash"], &h),
		dutil.AssignJSONHash(m["previous"], &previous),
		dutil.AssignJSONHash(m["proposal"], &proposal),
		dutil.AssignJSONHash(m["operations_tree"], &opsTree),
		dutil.AssignJSONHash(m["states_tree"], &statesTree),
		dutil.AssignJSONHash(m["suffrage"], &suffrage),
		dutil.AssignJSONInts(m["height"], &height),
		func() error {
			var s string

			if err := dutil.AssignJSON(m["proposed_at"], &s)(); err != nil {
				return err
			}

			var t localtime.Time
			if err := t.UnmarshalText([]byte(s)); err != nil {
				return err
			}

			proposedAt = t.Time

			return nil
		},
	); err != nil {
		return nil, err
	}

	return isaac.NewManifest(
			base.Height(height),
			previous,
			proposal,
			opsTree,
			statesTree,
			suffrage,
			proposedAt,
		),
		nil
}
