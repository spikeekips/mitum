module github.com/spikeekips/mitum

go 1.17

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/alecthomas/kong v0.5.0
	github.com/beevik/ntp v0.3.0
	github.com/bluele/gcache v0.0.2
	github.com/btcsuite/btcd v0.22.0-beta
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/bytedance/sonic v1.2.1
	github.com/go-redis/redis/v8 v8.11.5
	github.com/hashicorp/memberlist v0.3.1
	github.com/json-iterator/go v1.1.12
	github.com/lucas-clemente/quic-go v0.25.0
	github.com/mattn/go-isatty v0.0.14
	github.com/oklog/ulid v1.3.1
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.26.1
	github.com/satori/go.uuid v1.2.0
	github.com/stretchr/testify v1.7.1
	github.com/syndtr/goleveldb v0.0.0-20210819022825-2ae1ddf74ef7
	github.com/zeebo/blake3 v0.2.2
	go.uber.org/goleak v1.1.12
	golang.org/x/crypto v0.0.0-20220315160706-3147a52a75dd
	golang.org/x/mod v0.5.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
)

require (
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cheekybits/genny v1.0.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20211229061535-45e1f0233683 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/go-task/slim-sprig v0.0.0-20210107165309-348f09dbbbc0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.3 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.12 // indirect
	github.com/marten-seemann/qtls-go1-16 v0.1.4 // indirect
	github.com/marten-seemann/qtls-go1-17 v0.1.0 // indirect
	github.com/marten-seemann/qtls-go1-18 v0.1.0-beta.1 // indirect
	github.com/miekg/dns v1.1.26 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/lint v0.0.0-20201208152925-83fdc39ff7b5 // indirect
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f // indirect
	golang.org/x/sys v0.0.0-20220319134239-a9b59b0215f8 // indirect
	golang.org/x/tools v0.1.7 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/hashicorp/memberlist => github.com/spikeekips/memberlist v0.0.0-20220322022538-c65862450c13
