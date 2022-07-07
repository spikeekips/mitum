package base

import "github.com/spikeekips/mitum/util"

var objcache *util.GCacheObjectPool

func init() {
	objcache = util.NewGCacheObjectPool(1 << 13) //nolint:gomnd //...
}
