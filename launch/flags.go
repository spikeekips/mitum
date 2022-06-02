package launch

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/util"
)

type AddressFlag struct {
	address base.Address
}

func (f *AddressFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse address flag")

	address, err := base.ParseStringAddress(string(b))
	if err != nil {
		return e(err, "")
	}

	if err := address.IsValid(nil); err != nil {
		return e(err, "")
	}

	f.address = address

	return nil
}

func (f AddressFlag) Address() base.Address {
	return f.address
}

type ConnInfoFlag struct {
	ci network.ConnInfo
}

func (f *ConnInfoFlag) UnmarshalText(b []byte) error {
	e := util.StringErrorFunc("failed to parse ConnInfo flag")

	ci, err := network.ParseConnInfo(string(b))
	if err != nil {
		return e(err, "")
	}

	f.ci = ci

	return nil
}

func (f *ConnInfoFlag) ConnInfo() network.ConnInfo {
	return f.ci
}
