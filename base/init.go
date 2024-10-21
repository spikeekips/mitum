package base

import "github.com/spikeekips/mitum/util"

var objcache util.GCache[string, any]

func init() {
	objcache = util.NewLRUGCache[string, any](1 << 13) //nolint:mnd //...
}

func SetObjCache(c util.GCache[string, any]) {
	objcache = c
}
