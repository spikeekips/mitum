package launchcmd

import (
	"context"

	"github.com/pkg/errors"
)

type NetworkClientSetAllowConsensusCommand struct{} // FIXME remove next update(now Tue 29 Aug 2023 11:51:54 PM KST)

func (*NetworkClientSetAllowConsensusCommand) Run(context.Context) error {
	//revive:disable:line-length-limit
	return errors.Errorf("deprecated; use `network client node write [...] states.allow_consensus {true false}`")
	//revive:enable:line-length-limit
}
