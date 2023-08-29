package launchcmd

import (
	"context"

	"github.com/pkg/errors"
)

type NetworkClientSetAllowConsensusCommand struct{} // FIXME remove next update(now Tue 29 Aug 2023 11:51:54 PM KST)

func (*NetworkClientSetAllowConsensusCommand) Run(context.Context) error {
	return errors.Errorf("deprecated; use `network client design write allow_consensus {true false}`") // FIXME design -> node
}
