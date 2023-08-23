package launchcmd

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"

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

type extractFunc func(key, raw []byte) (map[string]interface{}, error)

type DatabaseExtractCommand struct { //nolint:govet //...
	// revive:disable:line-length-limit
	Storage         string   `arg:"" name:"storage" help:"storage base directory" type:"existingdir" default:"./"`
	Database        string   `arg:"" name:"database" help:"database directory" type:"existingdir" default:"./db"`
	Count           bool     `name:"count" help:"count by prefix"`
	Label           []string `name:"label" help:"label"`
	Prefix          []string `name:"prefix" help:"prefix"`
	Limit           uint64   `name:"limit" help:"limit result"`
	Raw             bool     `name:"raw" help:"raw result"`
	Bytes           string   `name:"bytes" help:"bytes encoder" default:"hex"`
	PrintLabelsKeys bool     `name:"print-labels-and-keys" help:"print available labels and keys"`
	log             *logging.Logging
	encs            *encoder.Encoders
	enc             encoder.Encoder
	st              *leveldbstorage.Storage
	filterLabel     func(string) bool
	filterPrefix    func(string) bool
	extracts        map[string]extractFunc
	encodeBytes     func([]byte) string
}

func (cmd *DatabaseExtractCommand) Run(pctx context.Context) error {
	if cmd.PrintLabelsKeys {
		cmd.printLabelsKeys()

		return nil
	}

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
	if err := util.LoadFromContextOK(pctx,
		launch.EncodersContextKey, &cmd.encs,
		launch.EncoderContextKey, &cmd.enc,
	); err != nil {
		return pctx, err
	}

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
		fsnodeinfo, cmd.Database, cmd.Storage, cmd.encs, cmd.enc)
	if err != nil {
		return pctx, err
	}

	cmd.st = st

	return pctx, nil
}

func (*DatabaseExtractCommand) printLabelsKeys() {
	malllabels := isaacdatabase.AllLabelKeys()
	alllabels := make([]string, len(malllabels))

	var i int

	for k := range malllabels {
		alllabels[i] = malllabels[k]
		i++
	}

	sort.Strings(alllabels)

	_, _ = fmt.Fprintln(os.Stdout, "labels")

	for i := range alllabels {
		_, _ = fmt.Fprintln(os.Stdout, "  -", alllabels[i])
	}

	mallprefixes := isaacdatabase.AllPrefixKeys()
	allprefixes := make([]string, len(mallprefixes))

	i = 0

	for k := range mallprefixes {
		allprefixes[i] = mallprefixes[k]
		i++
	}

	sort.Strings(allprefixes)

	_, _ = fmt.Fprintln(os.Stdout, "\nprefixes")

	for i := range allprefixes {
		_, _ = fmt.Fprintln(os.Stdout, "  -", allprefixes[i])
	}
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

	alllabels := isaacdatabase.AllLabelKeys()
	for k := range alllabels {
		label := alllabels[k]
		if !cmd.filterLabel(label) {
			continue
		}

		labels[label] = 0

		keyf := func(key []byte) []byte { return key }

		if label == "block_write" {
			keyf = func(key []byte) []byte { return key[34:] }
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
	keyf func([]byte) []byte,
) (count uint64, prefixes map[string]uint64, _ error) {
	prefixes = map[string]uint64{}

	all := isaacdatabase.AllPrefixKeys()

	pst := leveldbstorage.NewPrefixStorage(cmd.st, label[:])
	if err := pst.Iter(
		nil,
		func(key, raw []byte) (bool, error) {
			count++

			k := keyf(key)

			switch i, found := all[leveldbstorage.KeyPrefix(k[:2])]; {
			case !found:
				prefixes["unknown"]++
			case !cmd.filterPrefix(i):
				return true, nil
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

	alllabels := isaacdatabase.AllLabelKeys()
	for k := range alllabels {
		label := alllabels[k]
		if !cmd.filterLabel(label) {
			continue
		}

		keyf := func(key []byte) []byte { return key }

		if label == "block_write" {
			keyf = func(key []byte) []byte { return key[34:] }
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
	keyf func([]byte) []byte,
	left uint64,
) (uint64, error) {
	all := isaacdatabase.AllPrefixKeys()

	var count uint64

	pst := leveldbstorage.NewPrefixStorage(cmd.st, label[:])
	if err := pst.Iter(
		nil,
		func(key, raw []byte) (bool, error) {
			k := keyf(key)

			switch i, found := all[leveldbstorage.KeyPrefix(k[:2])]; {
			case !found:
				return true, nil
			case !cmd.filterPrefix(i):
				return true, nil
			default:
				if err := cmd.extractValue(i, key, raw); err != nil {
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

func (cmd *DatabaseExtractCommand) extractValue(prefix string, key, raw []byte) error {
	f := func(prefix string, key, raw []byte) (map[string]interface{}, error) {
		f, found := cmd.extracts[prefix]
		if !found {
			return nil, util.ErrNotFound.WithStack()
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

	m["prefix"] = prefix
	if len(comment) > 0 {
		m["comment"] = comment
	}

	cmd.printValue(m)

	return nil
}

func (cmd *DatabaseExtractCommand) extractHinted(key, raw []byte, v interface{}) (map[string]interface{}, error) {
	enchint, bmeta, body, err := isaacdatabase.ReadDatabaseHeader(raw)
	if err != nil {
		return nil, err
	}

	switch _, enc, found, err := cmd.encs.FindByString(enchint); {
	case err != nil:
		return nil, err
	case !found:
		return nil, util.ErrNotFound.Errorf("encoder not found for %q", enchint)
	default:
		if err := encoder.Decode(enc, body, &v); err != nil {
			return nil, err
		}
	}

	var meta map[string]interface{}

	if len(bmeta) > 0 {
		meta = map[string]interface{}{}

		switch h, err := isaacdatabase.ReadHashRecordMeta(bmeta); {
		case err != nil:
			return nil, err
		default:
			meta["hash"] = h
		}
	}

	return map[string]interface{}{
		"enchint": enchint,
		"key":     cmd.encodeBytes(key),
		"meta":    meta,
		"body": map[string]interface{}{
			"type": "hinted",
			"data": v,
		},
	}, nil
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
	return cmd.extractHinted(key, raw, new(base.State))
}

func (cmd *DatabaseExtractCommand) extractBlockMap(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHinted(key, raw, new(base.BlockMap))
}

func (cmd *DatabaseExtractCommand) extractInStateOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractKnownOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractProposal(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHinted(key, raw, new(base.ProposalSignFact))
}

func (cmd *DatabaseExtractCommand) extractProposalByPoint(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHash(key, raw)
}

func (cmd *DatabaseExtractCommand) extractNewOperation(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHinted(key, raw, new(base.Operation))
}

func (cmd *DatabaseExtractCommand) extractNewOperationOrdered(key, raw []byte) (map[string]interface{}, error) {
	switch h, err := isaacdatabase.ReadPoolOperationRecordMeta(raw); {
	case err != nil:
		return nil, err
	default:
		return map[string]interface{}{
			"key": cmd.encodeBytes(key),
			"meta": map[string]interface{}{
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
	return cmd.extractHinted(key, raw, new(base.BlockMap))
}

func (cmd *DatabaseExtractCommand) extractSuffrageProof(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHinted(key, raw, new(base.SuffrageProof))
}

func (cmd *DatabaseExtractCommand) extractSuffrageExpelOperation(key, raw []byte) (map[string]interface{}, error) {
	r, left, err := util.ReadLengthedBytesSlice(raw)
	if err != nil {
		return nil, err
	}

	switch i, err := cmd.extractHinted(key, left, new(base.SuffrageExpelOperation)); {
	case err != nil:
		return nil, err
	default:
		i["meta"] = r

		return i, nil
	}
}

func (*DatabaseExtractCommand) extractTempMerged([]byte, []byte) (map[string]interface{}, error) {
	return nil, nil
}

func (cmd *DatabaseExtractCommand) extractBallot(key, raw []byte) (map[string]interface{}, error) {
	return cmd.extractHinted(key, raw, new(base.Ballot))
}
