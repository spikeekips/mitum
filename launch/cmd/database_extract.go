package launchcmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/launch"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	allDatabaseExtractLabels         map[leveldbstorage.KeyPrefix]string
	allDatabase2PrefixKeys           map[leveldbstorage.KeyPrefix]string
	allDatabaseEventPrefixKeyStrings []string
	allDatabaseEventPrefixKeys       map[[32]byte]string
)

func init() {
	allDatabaseExtractLabels = isaacdatabase.AllLabelKeys()
	allDatabaseExtractLabels[launch.LeveldbLabelEventDatabase] = "event"

	allDatabase2PrefixKeys = isaacdatabase.AllPrefixKeys()

	allDatabaseEventPrefixKeyStrings = make([]string, len(launch.AllEventLoggerNames)+1)
	allDatabaseEventPrefixKeys = map[[32]byte]string{}

	for i := range launch.AllEventLoggerNames {
		s := launch.AllEventLoggerNames[i]

		allDatabaseEventPrefixKeyStrings[i] = "event." + string(s)

		allDatabaseEventPrefixKeys[launch.EventNameKeyPrefix(s)] = "event." + string(s)
	}
}

type extractFunc func(key, raw []byte) (map[string]interface{}, error)

type DatabaseExtractCommand struct { //nolint:govet //...
	// revive:disable:line-length-limit
	Storage      string   `arg:"" name:"storage" help:"storage base directory" type:"existingdir" default:"./"`
	Database     string   `arg:"" name:"database" help:"database directory" type:"existingdir" default:"./db"`
	Count        bool     `name:"count" help:"count by prefix"`
	Label        []string `name:"label" help:"label"`
	Prefix       []string `name:"prefix" help:"prefix"`
	Limit        uint64   `name:"limit" help:"limit result"`
	Raw          bool     `name:"raw" help:"raw result"`
	Bytes        string   `name:"bytes" help:"bytes encoder" default:"hex"`
	log          *logging.Logging
	encs         *encoder.Encoders
	enc          encoder.Encoder
	defaultst    *leveldbstorage.Storage
	eventst      *leveldbstorage.Storage
	filterLabel  func(string) bool
	filterPrefix func(string) bool
	extracts     map[string]extractFunc
	encodeBytes  func([]byte) string
	// revive:enable:line-length-limit
}

func (cmd *DatabaseExtractCommand) Run(pctx context.Context) error {
	if err := cmd.prepare(pctx); err != nil {
		return err
	}

	cmd.log.Log().Debug().
		Interface("storage", cmd.Storage).
		Interface("database", cmd.Database).
		Strs("labels", cmd.Label).
		Strs("prefixes", cmd.Prefix).
		Msg("flags")

	pps := ps.NewPS("cmd-database-extract")
	_ = pps.SetLogging(cmd.log)

	_ = pps.
		AddOK(launch.PNameEncoder, launch.PEncoder, nil).
		AddOK(launch.PNameStorage, launch.PStorage, launch.PCloseStorage, launch.PNameEncoder)

	_ = pps.POK(launch.PNameEncoder).
		PostAddOK(launch.PNameAddHinters, launch.PAddHinters)

	_ = pps.POK(launch.PNameStorage).
		PostAddOK(launch.PNameLoadFromDatabase, cmd.pLoadDatabase)

	cmd.log.Log().Debug().Interface("process", pps.Verbose()).Msg("process ready")

	nctx, err := pps.Run(pctx)
	defer func() {
		cmd.log.Log().Debug().Interface("process", pps.Verbose()).Msg("process will be closed")

		if _, err = pps.Close(nctx); err != nil {
			cmd.log.Log().Error().Err(err).Msg("failed to close")
		}
	}()

	switch {
	case err != nil:
		return err
	case cmd.Count:
		return cmd.count(nctx)
	default:
		return cmd.extract(nctx)
	}
}

func (cmd *DatabaseExtractCommand) prepare(pctx context.Context) error {
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &cmd.log); err != nil {
		return err
	}

	if err := cmd.preparePath(pctx); err != nil {
		return err
	}

	if err := cmd.prepareFilters(pctx); err != nil {
		return err
	}

	switch cmd.Bytes {
	case "base64":
		cmd.encodeBytes = func(b []byte) string {
			return base64.URLEncoding.EncodeToString(b)
		}
	case "go":
		cmd.encodeBytes = func(b []byte) string {
			return fmt.Sprintf("%v", b)
		}
	case "hex":
		cmd.encodeBytes = func(b []byte) string {
			return hex.EncodeToString(b)
		}
	default:
		return errors.Errorf("unknown --bytes, %q", cmd.Bytes)
	}

	return nil
}

func (cmd *DatabaseExtractCommand) preparePath(context.Context) error {
	switch i, err := filepath.Abs(filepath.Clean(cmd.Storage)); {
	case err != nil:
		return errors.WithStack(err)
	default:
		cmd.Storage = i
	}

	if len(cmd.Database) < 1 {
		cmd.Database = launch.LocalFSDatabaseDirectory(cmd.Storage)
	}

	switch fi, err := os.Stat(cmd.Storage); {
	case err != nil:
		return errors.WithStack(err)
	case !fi.IsDir():
		return errors.Errorf("storage, %q not directory", cmd.Storage)
	}

	switch fi, err := os.Stat(cmd.Database); {
	case err != nil:
		return errors.WithStack(err)
	case !fi.IsDir():
		return errors.Errorf("database, %q not directory", cmd.Database)
	}

	return nil
}

func (cmd *DatabaseExtractCommand) prepareFilters(context.Context) error {
	cmd.filterLabel = func(string) bool { return true }

	if len(cmd.Label) > 0 {
		m := map[string]struct{}{}
		for i := range cmd.Label {
			m[cmd.Label[i]] = struct{}{}
		}

		cmd.filterLabel = func(label string) bool {
			_, found := m[label]

			return found
		}
	}

	cmd.filterPrefix = func(string) bool { return true }

	if len(cmd.Prefix) > 0 {
		m := map[string]struct{}{}
		for i := range cmd.Prefix {
			m[cmd.Prefix[i]] = struct{}{}
		}

		cmd.filterPrefix = func(label string) bool {
			_, found := m[label]

			return found
		}
	}

	return nil
}

func (cmd *DatabaseExtractCommand) pLoadDatabase(pctx context.Context) (context.Context, error) {
	if err := util.LoadFromContextOK(pctx, launch.EncodersContextKey, &cmd.encs); err != nil {
		return pctx, err
	}

	cmd.enc = cmd.encs.Default()

	var fsnodeinfo launch.NodeInfo

	switch i, found, err := launch.LoadNodeInfo(cmd.Storage, cmd.enc); {
	case err != nil:
		return pctx, err
	case !found:
		return pctx, util.ErrNotFound.Errorf("fs node info")
	default:
		fsnodeinfo = i

		cmd.log.Log().Info().Interface("fs_node_info", fsnodeinfo).Msg("fs node info loaded")
	}

	switch i, err := leveldbStorage.OpenFile(cmd.Database, true); {
	case err != nil:
		return pctx, errors.WithStack(err)
	default:
		_ = i.Close()
	}

	st, _, _, _, err := launch.LoadDatabase(
		fsnodeinfo, cmd.Database, cmd.Storage, cmd.encs, cmd.enc, 0, 0)
	if err != nil {
		return pctx, err
	}

	cmd.defaultst = st

	switch st, err := launch.LoadDefaultEventStorage(launch.LocalFSEventDatabaseDirectory(cmd.Storage), true); {
	case os.IsNotExist(err):
	case err != nil:
		return pctx, err
	default:
		cmd.eventst = st
	}

	return pctx, nil
}

func (*DatabaseExtractCommand) printValue(i interface{}) {
	switch b, err := util.MarshalJSON(i); {
	case err != nil:
	default:
		_, _ = fmt.Fprintln(os.Stdout, string(b))
	}
}

func (cmd *DatabaseExtractCommand) count(context.Context) error {
	var total uint64

	labels := map[string]interface{}{}

	for k := range allDatabaseExtractLabels {
		label := allDatabaseExtractLabels[k]
		if !cmd.filterLabel(label) {
			continue
		}

		labels[label] = 0

		var keyf func(key []byte) string

		switch label {
		case "event":
			keyf = func(key []byte) string {
				return allDatabaseEventPrefixKeys[[32]byte(key[:32])]
			}
		case "block_write":
			keyf = func(key []byte) string {
				return allDatabase2PrefixKeys[leveldbstorage.KeyPrefix(key[34:][:2])]
			}
		default:
			keyf = func(key []byte) string {
				return allDatabase2PrefixKeys[leveldbstorage.KeyPrefix(key[:2])]
			}
		}

		switch count, prefixes, err := cmd.countLabel(
			k,
			keyf,
		); {
		case err != nil:
			return err
		default:
			total += count

			labels[label] = prefixes
		}
	}

	cmd.log.Log().Info().Uint64("count", total).Msg("extracted")

	cmd.printValue(labels)

	return nil
}

func (cmd *DatabaseExtractCommand) countLabel(
	label leveldbstorage.KeyPrefix,
	keyf func([]byte) string,
) (count uint64, prefixes map[string]uint64, _ error) {
	prefixes = map[string]uint64{}

	st := cmd.st(label)
	if st == nil {
		return 0, nil, nil
	}

	pst := leveldbstorage.NewPrefixStorage(st, label[:])
	if err := pst.Iter(
		nil,
		func(key, raw []byte) (bool, error) {
			count++

			switch i := keyf(key); {
			case !cmd.filterPrefix(i):
				return true, nil
			case len(i) < 1:
				prefixes["unknown"]++
			default:
				prefixes[i]++
			}

			return true, nil
		}, false); err != nil {
		return 0, nil, err
	}

	prefixes["_total"] = count

	return count, prefixes, nil
}

func (cmd *DatabaseExtractCommand) extract(context.Context) error {
	cmd.extracts = map[string]extractFunc{
		"blockmap":                       cmd.extractBlockMap,
		"state":                          cmd.extractState,
		"in_state_operation":             cmd.extractInStateOperation,
		"known_operation":                cmd.extractKnownOperation,
		"proposal":                       cmd.extractProposal,
		"proposal_by_point":              cmd.extractProposalByPoint,
		"new_operation":                  cmd.extractNewOperation,
		"new_operation_ordered":          cmd.extractNewOperationOrdered,
		"new_operation_ordered_keys":     cmd.extractNewOperationOrderedKeys,
		"removed_new_operation":          cmd.extractRemovedNewOperation,
		"temp_sync_map":                  cmd.extractTempSyncMap,
		"suffrage_proof":                 cmd.extractSuffrageProof,
		"suffrage_proof_by_block_height": cmd.extractSuffrageProof,
		"suffrage_expel_operation":       cmd.extractSuffrageExpelOperation,
		"temp_merged":                    cmd.extractTempMerged,
		"ballot":                         cmd.extractBallot,
	}

	left := cmd.Limit

	var total uint64

	for k := range allDatabaseExtractLabels {
		label := allDatabaseExtractLabels[k]
		if !cmd.filterLabel(label) {
			continue
		}

		var keyf func(key []byte) string

		switch label {
		case "event":
			keyf = func(key []byte) string {
				return allDatabaseEventPrefixKeys[[32]byte(key[:32])]
			}
		case "block_write":
			keyf = func(key []byte) string {
				return allDatabase2PrefixKeys[leveldbstorage.KeyPrefix(key[34:][:2])]
			}
		default:
			keyf = func(key []byte) string {
				return allDatabase2PrefixKeys[leveldbstorage.KeyPrefix(key[:2])]
			}
		}

		switch count, err := cmd.extractLabel(k, keyf, left); {
		case err != nil:
			return err
		default:
			total += count
		}

		if left > 0 {
			if left = left - total; left < 1 {
				break
			}
		}
	}

	cmd.log.Log().Info().Uint64("count", total).Msg("extracted")

	return nil
}

func (cmd *DatabaseExtractCommand) extractLabel(
	label leveldbstorage.KeyPrefix,
	keyf func([]byte) string,
	left uint64,
) (uint64, error) {
	var count uint64

	labelstring := allDatabaseExtractLabels[label]

	st := cmd.st(label)
	if st == nil {
		return 0, nil
	}

	pst := leveldbstorage.NewPrefixStorage(st, label[:])
	if err := pst.Iter(
		nil,
		func(key, raw []byte) (bool, error) {
			i := keyf(key)

			switch {
			case len(i) < 1:
				return true, nil
			case !cmd.filterPrefix(i):
				return true, nil
			default:
				if err := cmd.extractValue(labelstring, i, key, raw); err != nil {
					return false, err
				}

				count++

				if left > 0 && count == left {
					return false, nil
				}
			}

			return true, nil
		}, false); err != nil {
		return 0, err
	}

	return count, nil
}

func (cmd *DatabaseExtractCommand) extractValue(label, prefix string, key, raw []byte) error {
	f := func(prefix string, key, raw []byte) (map[string]interface{}, error) {
		var f extractFunc

		switch {
		case prefix == "event.all":
			f = cmd.extractBytes
		case strings.HasPrefix(prefix, "event."):
			f = cmd.extractNamedEvent
		default:
			i, found := cmd.extracts[prefix]
			if !found {
				return nil, util.ErrNotFound.WithStack()
			}

			f = i
		}

		return f(key, raw)
	}

	var m map[string]interface{}
	var err error

	if !cmd.Raw {
		m, err = f(prefix, key, raw)
	}

	var comment string

	switch {
	case errors.Is(err, util.ErrNotFound):
		comment = "unknown"
	case err != nil:
		comment = fmt.Sprintf("error: %q", err)
	}

	if m == nil {
		m = map[string]interface{}{
			"key": cmd.encodeBytes(key),
			"body": map[string]interface{}{
				"type": "raw",
				"data": cmd.encodeBytes(raw),
			},
		}
	}

	m["label"] = label
	m["prefix"] = prefix

	if len(comment) > 0 {
		m["comment"] = comment
	}

	cmd.printValue(m)

	return nil
}

func (cmd *DatabaseExtractCommand) extractHinted(key, raw []byte, v interface{}) (map[string]interface{}, error) {
	enchint, headers, body, err := isaacdatabase.ReadFrame(raw)
	if err != nil {
		return nil, err
	}

	if err := isaacdatabase.DecodeFrame(cmd.encs, enchint, body, &v); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"enchint": enchint,
		"key":     cmd.encodeBytes(key),
		"header":  headers,
		"body": map[string]interface{}{
			"type": "hinted",
			"data": v,
		},
	}, nil
}

func (cmd *DatabaseExtractCommand) extractHintedHashHeader(
	key, raw []byte, v interface{},
) (map[string]interface{}, error) {
	m, err := cmd.extractHinted(key, raw, v)
	if err != nil {
		return nil, err
	}

	if h := m["header"].([][]byte); len(h) > 0 { //nolint:forcetypeassert //...
		m["meta"] = map[string]interface{}{
			"hash": valuehash.NewBytes(h[0]),
		}
	}

	return m, nil
}

func (cmd *DatabaseExtractCommand) extractHash(key, raw []byte) (map[string]interface{}, error) {
	return map[string]interface{}{
		"key": cmd.encodeBytes(key),
		"body": map[string]interface{}{
			"type": "hash",
			"data": valuehash.NewBytes(raw),
		},
	}, nil
}

func (cmd *DatabaseExtractCommand) extractBytes(key, raw []byte) (map[string]interface{}, error) {
	return map[string]interface{}{
		"key": cmd.encodeBytes(key),
		"body": map[string]interface{}{
			"type": "bytes",
			"data": cmd.encodeBytes(raw),
		},
	}, nil
}

func (cmd *DatabaseExtractCommand) extractState(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHintedHashHeader(key, raw, new(base.State))
}

func (cmd *DatabaseExtractCommand) extractBlockMap(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHintedHashHeader(key, raw, new(base.BlockMap))
}

func (cmd *DatabaseExtractCommand) extractInStateOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractKnownOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractProposal(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHintedHashHeader(key, raw, new(base.ProposalSignFact))
}

func (cmd *DatabaseExtractCommand) extractProposalByPoint(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractNewOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHintedHashHeader(key, raw, new(base.Operation))
}

func (cmd *DatabaseExtractCommand) extractNewOperationOrdered(key, raw []byte) (map[string]interface{}, error) {
	switch h, err := isaacdatabase.ReadFrameHeaderOperation(raw); {
	case err != nil:
		return nil, err
	default:
		return map[string]interface{}{
			"key": cmd.encodeBytes(key),
			"header": map[string]interface{}{
				"version":   h.Version(),
				"added_at":  h.AddedAt(),
				"hint":      h.Hint(),
				"operation": h.Operation(),
				"fact":      h.Fact(),
			},
		}, nil
	}
}

func (cmd *DatabaseExtractCommand) extractNewOperationOrderedKeys(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractBytes(key, raw)
}

func (cmd *DatabaseExtractCommand) extractRemovedNewOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractTempSyncMap(key, raw []byte) (map[string]interface{}, error) {
	body, err := isaacdatabase.ReadNoHeadersFrame(raw)
	if err != nil {
		return nil, err
	}

	var v base.BlockMap

	if err := isaacdatabase.DecodeFrame(cmd.encs, cmd.enc.Hint().String(), body, &v); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"enchint": cmd.enc.Hint(),
		"key":     cmd.encodeBytes(key),
		"body": map[string]interface{}{
			"type": "hinted",
			"data": v,
		},
	}, nil
}

func (cmd *DatabaseExtractCommand) extractSuffrageProof(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHintedHashHeader(key, raw, new(base.SuffrageProof))
}

func (cmd *DatabaseExtractCommand) extractSuffrageExpelOperation(key, raw []byte) (map[string]interface{}, error) {
	enchint, r, left, err := isaacdatabase.ReadFrameHeaderSuffrageExpelOperation(raw)
	if err != nil {
		return nil, err
	}

	var v base.SuffrageExpelOperation
	if err := isaacdatabase.DecodeFrame(cmd.encs, enchint, left, &v); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"enchint": enchint,
		"key":     cmd.encodeBytes(key),
		"meta": map[string]interface{}{
			"node":  r.Node(),
			"start": r.Start(),
			"end":   r.End(),
		},
		"body": map[string]interface{}{
			"type": "hinted",
			"data": v,
		},
	}, nil
}

func (*DatabaseExtractCommand) extractTempMerged([]byte, []byte) (map[string]interface{}, error) {
	return nil, nil
}

func (cmd *DatabaseExtractCommand) extractBallot(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHinted(key, raw, new(base.Ballot))
}

func (cmd *DatabaseExtractCommand) extractNamedEvent(key, raw []byte) (m map[string]interface{}, _ error) {
	var t time.Time
	var offset int64

	switch i, j, err := launch.LoadEventInfoFromKey(key); {
	case err != nil:
		return nil, err
	default:
		t = i
		offset = j
	}

	switch b, err := launch.LoadRawEvent(raw); {
	case err != nil:
		return nil, err
	default:
		return map[string]interface{}{
			"key": cmd.encodeBytes(key),
			"meta": map[string]interface{}{
				"time":   t,
				"offset": offset,
			},
			"body": json.RawMessage(b),
		}, nil
	}
}

func (*DatabaseExtractCommand) Help() string {
	alllabels := make([]string, len(allDatabaseExtractLabels))

	var i int

	for k := range allDatabaseExtractLabels {
		alllabels[i] = allDatabaseExtractLabels[k]
		i++
	}

	sort.Strings(alllabels)

	buf := bytes.NewBuffer(nil)

	_, _ = fmt.Fprintln(buf, "## labels")

	for i := range alllabels {
		_, _ = fmt.Fprintln(buf, "  - ", alllabels[i])
	}

	mall2prefixes := allDatabase2PrefixKeys
	allprefixes := make([]string, len(allDatabase2PrefixKeys)+len(allDatabaseEventPrefixKeyStrings))

	i = 0

	for k := range allDatabaseEventPrefixKeyStrings {
		allprefixes[i] = allDatabaseEventPrefixKeyStrings[k]
		i++
	}

	for k := range mall2prefixes {
		allprefixes[i] = mall2prefixes[k]
		i++
	}

	sort.Strings(allprefixes)

	_, _ = fmt.Fprintln(buf, "\n## prefixes")

	for i := range allprefixes {
		_, _ = fmt.Fprintln(buf, "  - ", allprefixes[i])
	}

	return buf.String()
}

func (cmd *DatabaseExtractCommand) st(label leveldbstorage.KeyPrefix) *leveldbstorage.Storage {
	switch label {
	case launch.LeveldbLabelEventDatabase:
		return cmd.eventst
	default:
		return cmd.defaultst
	}
}
