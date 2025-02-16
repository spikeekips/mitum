package main

import "github.com/spikeekips/mitum/util"

func (msg startWorkerMsg) String() string {
	return util.MustMarshalJSONString(msg)
}
