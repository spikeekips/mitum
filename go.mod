module github.com/spikeekips/mitum

go 1.18

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/alecthomas/kong v0.5.0
	github.com/beevik/ntp v0.3.0
	github.com/bluele/gcache v0.0.2
	github.com/btcsuite/btcd v0.22.1
	github.com/btcsuite/btcd/chaincfg/chainhash v1.0.1
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/bytedance/sonic v1.3.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/hashicorp/memberlist v0.3.1
	github.com/json-iterator/go v1.1.12
	github.com/lucas-clemente/quic-go v0.27.1
	github.com/mattn/go-isatty v0.0.14
	github.com/oklog/ulid v1.3.1
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.26.1
	github.com/satori/go.uuid v1.2.0
	github.com/stretchr/testify v1.7.1
	github.com/syndtr/goleveldb v0.0.0-20210819022825-2ae1ddf74ef7
	github.com/zeebo/blake3 v0.2.3
	go.uber.org/goleak v1.1.12
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e
	golang.org/x/mod v0.6.0-dev.0.20220106191415-9b9b3d81d5e3
	golang.org/x/sync v0.0.0-20220513210516-0976fa681c29
)

require (
	github.com/armon/go-metrics v0.4.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cheekybits/genny v1.0.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20220526154910-8bf9453eb81a // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/go-task/slim-sprig v0.0.0-20210107165309-348f09dbbbc0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/marten-seemann/qtls-go1-16 v0.1.5 // indirect
	github.com/marten-seemann/qtls-go1-17 v0.1.1 // indirect
	github.com/marten-seemann/qtls-go1-18 v0.1.1 // indirect
	github.com/miekg/dns v1.1.49 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.0.0-20220412001346-fc48f9fe4c15 // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/net v0.0.0-20220526153639-5463443f8c37 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	golang.org/x/tools v0.1.10 // indirect
	golang.org/x/xerrors v0.0.0-20220517211312-f3a8303e98df // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/hashicorp/memberlist => github.com/spikeekips/memberlist v0.0.0-20220322022538-c65862450c13
