module github.com/spikeekips/mitum

go 1.21

require (
	github.com/Masterminds/semver/v3 v3.2.1
	github.com/alecthomas/kong v0.8.1
	github.com/arl/statsviz v0.6.0
	github.com/beevik/ntp v1.3.0
	github.com/bluele/gcache v0.0.2
	github.com/btcsuite/btcd/btcec/v2 v2.3.2
	github.com/btcsuite/btcd/chaincfg/chainhash v1.0.2
	github.com/bytedance/sonic v1.10.2
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/hashicorp/consul/api v1.26.1
	github.com/hashicorp/memberlist v0.5.1-0.20230206163934-e5eb6d78d6f2
	github.com/hashicorp/vault/api v1.10.0
	github.com/json-iterator/go v1.1.12
	github.com/mattn/go-isatty v0.0.20
	github.com/oklog/ulid/v2 v2.1.0
	github.com/pkg/errors v0.9.1
	github.com/quic-go/quic-go v0.40.0
	github.com/redis/go-redis/v9 v9.3.0
	github.com/rs/zerolog v1.31.0
	github.com/stretchr/testify v1.8.4
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	github.com/zeebo/blake3 v0.2.3
	go.uber.org/goleak v1.3.0
	golang.org/x/crypto v0.15.0
	golang.org/x/exp v0.0.0-20231110203233-9a3e6036ecaa
	golang.org/x/mod v0.14.0
	golang.org/x/sync v0.5.0
	golang.org/x/time v0.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20230717121745-296ad89f973d // indirect
	github.com/chenzhuoyu/iasm v0.9.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-jose/go-jose/v3 v3.0.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/pprof v0.0.0-20231101202521-4ca4178f5c7a // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.5.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.5 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-secure-stdlib/parseutil v0.1.8 // indirect
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.6 // indirect
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/klauspost/cpuid/v2 v2.2.6 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/miekg/dns v1.1.56 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/ginkgo/v2 v2.13.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/quic-go/qtls-go1-20 v0.4.1 // indirect
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/stretchr/objx v0.5.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	go.uber.org/mock v0.3.0 // indirect
	golang.org/x/arch v0.6.0 // indirect
	golang.org/x/net v0.18.0 // indirect
	golang.org/x/sys v0.14.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/tools v0.15.0 // indirect
	google.golang.org/protobuf v1.29.1 // indirect
)

replace github.com/hashicorp/memberlist => github.com/spikeekips/memberlist v0.0.0-20230626195851-39f17fa10d23 // latest fix-data-race branch
