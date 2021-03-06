package ballot

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/key"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BaseBallotV0PackerJSON struct {
	jsonenc.HintedHead
	H   valuehash.Hash `json:"hash"`
	SN  key.Publickey  `json:"signer"`
	SG  key.Signature  `json:"signature"`
	SA  localtime.Time `json:"signed_at"`
	HT  base.Height    `json:"height"`
	RD  base.Round     `json:"round"`
	N   base.Address   `json:"node"`
	BH  valuehash.Hash `json:"body_hash"`
	FSG key.Signature  `json:"fact_signature"`
}

func PackBaseBallotV0JSON(ballot Ballot) (BaseBallotV0PackerJSON, error) {
	return BaseBallotV0PackerJSON{
		HintedHead: jsonenc.NewHintedHead(ballot.Hint()),
		H:          ballot.Hash(),
		SN:         ballot.Signer(),
		SG:         ballot.Signature(),
		SA:         localtime.NewTime(ballot.SignedAt()),
		HT:         ballot.Height(),
		RD:         ballot.Round(),
		N:          ballot.Node(),
		BH:         ballot.BodyHash(),
		FSG:        ballot.FactSignature(),
	}, nil
}

type BaseBallotV0UnpackerJSON struct {
	jsonenc.HintedHead
	H   valuehash.Bytes      `json:"hash"`
	SN  key.PublickeyDecoder `json:"signer"`
	SG  key.Signature        `json:"signature"`
	SA  localtime.Time       `json:"signed_at"`
	HT  base.Height          `json:"height"`
	RD  base.Round           `json:"round"`
	N   base.AddressDecoder  `json:"node"`
	BH  valuehash.Bytes      `json:"body_hash"`
	FSG key.Signature        `json:"fact_signature"`
}

func UnpackBaseBallotV0JSON(nib BaseBallotV0UnpackerJSON, enc *jsonenc.Encoder) (
	BaseBallotV0,
	BaseBallotFactV0,
	error,
) {
	// signer
	var signer key.Publickey
	if k, err := nib.SN.Encode(enc); err != nil {
		return BaseBallotV0{}, BaseBallotFactV0{}, err
	} else {
		signer = k
	}

	var node base.Address
	if n, err := nib.N.Encode(enc); err != nil {
		return BaseBallotV0{}, BaseBallotFactV0{}, err
	} else {
		node = n
	}

	var h, bh valuehash.Hash
	if !nib.H.Empty() {
		h = nib.H
	}
	if !nib.BH.Empty() {
		bh = nib.BH
	}

	return BaseBallotV0{
			h:             h,
			bodyHash:      bh,
			signer:        signer,
			signature:     nib.SG,
			signedAt:      nib.SA.Time,
			node:          node,
			factSignature: nib.FSG,
		},
		BaseBallotFactV0{
			height: nib.HT,
			round:  nib.RD,
		}, nil
}

type BaseBallotFactV0PackerJSON struct {
	jsonenc.HintedHead
	HT base.Height `json:"height"`
	RD base.Round  `json:"round"`
}

func NewBaseBallotFactV0PackerJSON(bbf BaseBallotFactV0, ht hint.Hint) BaseBallotFactV0PackerJSON {
	return BaseBallotFactV0PackerJSON{
		HintedHead: jsonenc.NewHintedHead(ht),
		HT:         bbf.height,
		RD:         bbf.round,
	}
}
