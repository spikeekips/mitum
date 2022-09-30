package launch

import (
	"os"
	"path/filepath"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
)

var (
	DefaultNodeInfoHint = hint.MustNewHint("default-node-info-v0.0.1")
	NodeInfoFilename    = "info.json"
)

type NodeInfo interface {
	util.IsValider
	ID() string
	NetworkID() base.NetworkID
	CreatedAt() time.Time
	LastStartedAt() time.Time
	UpdateLastStartedAt() NodeInfo
	Version() util.Version // NOTE mitum build version
}

type DefaultNodeInfo struct {
	createdAt     time.Time
	lastStartedAt time.Time
	id            string
	networkID     base.NetworkID
	hint.BaseHinter
	version util.Version
}

func NewDefaultNodeInfo(id string, networkID base.NetworkID, version util.Version) DefaultNodeInfo {
	now := localtime.UTCNow()

	return DefaultNodeInfo{
		BaseHinter:    hint.NewBaseHinter(DefaultNodeInfoHint),
		id:            id,
		networkID:     networkID,
		createdAt:     now,
		lastStartedAt: now,
		version:       version,
	}
}

func CreateDefaultNodeInfo(networkID base.NetworkID, version util.Version) DefaultNodeInfo {
	return NewDefaultNodeInfo(util.ULID().String(), networkID, version)
}

func (info DefaultNodeInfo) ID() string {
	return info.id
}

func (info DefaultNodeInfo) NetworkID() base.NetworkID {
	return info.networkID
}

func (info DefaultNodeInfo) CreatedAt() time.Time {
	return info.createdAt
}

func (info DefaultNodeInfo) LastStartedAt() time.Time {
	return info.lastStartedAt
}

func (info DefaultNodeInfo) Version() util.Version {
	return info.version
}

func (info DefaultNodeInfo) UpdateLastStartedAt() NodeInfo {
	return DefaultNodeInfo{
		BaseHinter:    info.BaseHinter,
		id:            info.id,
		networkID:     info.networkID,
		createdAt:     info.createdAt,
		lastStartedAt: localtime.UTCNow(),
		version:       info.version,
	}
}

func (info DefaultNodeInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid DefaultNodeInfo")

	if err := info.BaseHinter.IsValid(DefaultNodeInfoHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, info.networkID, info.version); err != nil {
		return e.Wrap(err)
	}

	if len(info.id) < 1 {
		return e.Errorf("empty id")
	}

	if info.createdAt.IsZero() {
		return e.Errorf("empty created_at time")
	}

	if info.lastStartedAt.IsZero() {
		return e.Errorf("empty last_started_at time")
	}

	return nil
}

type defaultNodeInfoJSONMarshaler struct {
	ID            string         `json:"id"`
	CreatedAt     localtime.Time `json:"created_at"`
	NetworkID     base.NetworkID `json:"network_id"`
	LastStartedAt localtime.Time `json:"last_started_at"`
	hint.BaseHinter
	Version util.Version `json:"version"`
}

func (info DefaultNodeInfo) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(defaultNodeInfoJSONMarshaler{
		BaseHinter:    info.BaseHinter,
		ID:            info.id,
		NetworkID:     info.networkID,
		CreatedAt:     localtime.New(info.createdAt),
		LastStartedAt: localtime.New(info.lastStartedAt),
		Version:       info.version,
	})
}

type defaultNodeInfoJSONUnmarshaler struct {
	CreatedAt     localtime.Time `json:"created_at"`
	LastStartedAt localtime.Time `json:"last_started_at"`
	ID            string         `json:"id"`
	NetworkID     base.NetworkID `json:"network_id"`
	Version       util.Version   `json:"version"`
}

func (info *DefaultNodeInfo) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal DefaultNodeInfo")

	var u defaultNodeInfoJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	info.id = u.ID
	info.networkID = u.NetworkID
	info.createdAt = u.CreatedAt.Time
	info.lastStartedAt = u.LastStartedAt.Time
	info.version = u.Version

	return nil
}

func SaveNodeInfo(root string, i NodeInfo) error {
	e := util.StringErrorFunc("failed to save NodeInfo")

	b, err := util.MarshalJSON(i)
	if err != nil {
		return e(err, "")
	}

	f, err := os.OpenFile(
		filepath.Join(root, NodeInfoFilename),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o600,
	)
	if err != nil {
		return e(err, "")
	}

	if _, err := f.Write(b); err != nil {
		return e(err, "")
	}

	return nil
}

func LoadNodeInfo(root string, enc encoder.Encoder) (_ NodeInfo, found bool, _ error) {
	e := util.StringErrorFunc("failed to save NodeInfo")

	f, err := os.Open(filepath.Join(root, NodeInfoFilename))

	switch {
	case err == nil:
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, e(err, "")
	}

	defer func() {
		_ = f.Close()
	}()

	var i NodeInfo
	if err := encoder.DecodeReader(enc, f, &i); err != nil {
		return nil, true, e(err, "")
	}

	if err := i.IsValid(nil); err != nil {
		return nil, true, e(err, "")
	}

	i = i.UpdateLastStartedAt()
	if err := SaveNodeInfo(root, i); err != nil {
		return nil, true, e(err, "failed to update NodeInfo")
	}

	return i, true, nil
}
