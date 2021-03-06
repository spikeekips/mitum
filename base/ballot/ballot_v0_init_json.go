package ballot // nolint

import (
	"encoding/json"

	"github.com/spikeekips/mitum/base"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
)

type INITBallotV0PackerJSON struct {
	BaseBallotV0PackerJSON
	PB  valuehash.Hash `json:"previous_block"`
	VR  base.Voteproof `json:"voteproof"`
	AVR base.Voteproof `json:"accept_voteproof"`
}

func (ib INITBallotV0) MarshalJSON() ([]byte, error) {
	bb, err := PackBaseBallotV0JSON(ib)
	if err != nil {
		return nil, err
	}
	return jsonenc.Marshal(INITBallotV0PackerJSON{
		BaseBallotV0PackerJSON: bb,
		PB:                     ib.previousBlock,
		VR:                     ib.voteproof,
		AVR:                    ib.acceptVoteproof,
	})
}

type INITBallotV0UnpackerJSON struct {
	BaseBallotV0UnpackerJSON
	PB  valuehash.Bytes `json:"previous_block"`
	VR  json.RawMessage `json:"voteproof"`
	AVR json.RawMessage `json:"accept_voteproof"`
}

func (ib *INITBallotV0) UnpackJSON(b []byte, enc *jsonenc.Encoder) error {
	bb, bf, err := ib.BaseBallotV0.unpackJSON(b, enc)
	if err != nil {
		return err
	}

	var nib INITBallotV0UnpackerJSON
	if err := enc.Unmarshal(b, &nib); err != nil {
		return err
	}

	return ib.unpack(enc, bb, bf, nib.PB, nib.VR, nib.AVR)
}

type INITBallotFactV0PackerJSON struct {
	BaseBallotFactV0PackerJSON
	PB valuehash.Hash `json:"previous_block"`
}

func (ibf INITBallotFactV0) MarshalJSON() ([]byte, error) {
	return jsonenc.Marshal(INITBallotFactV0PackerJSON{
		BaseBallotFactV0PackerJSON: NewBaseBallotFactV0PackerJSON(ibf.BaseBallotFactV0, ibf.Hint()),
		PB:                         ibf.previousBlock,
	})
}

type INITBallotFactV0UnpackerJSON struct {
	BaseBallotFactV0PackerJSON
	PB valuehash.Bytes `json:"previous_block"`
}

func (ibf *INITBallotFactV0) UnpackJSON(b []byte, enc *jsonenc.Encoder) error {
	var err error

	var bf BaseBallotFactV0
	if bf, err = ibf.BaseBallotFactV0.unpackJSON(b, enc); err != nil {
		return err
	}

	var ubf INITBallotFactV0UnpackerJSON
	if err := enc.Unmarshal(b, &ubf); err != nil {
		return err
	}

	return ibf.unpack(enc, bf, ubf.PB)
}
