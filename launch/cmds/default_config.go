package cmds

import (
	"context"
	"fmt"
	"io"
	"os"

	"golang.org/x/xerrors"
	"gopkg.in/yaml.v3"

	"github.com/alecthomas/kong"
	"github.com/spikeekips/mitum/launch/config"
	"github.com/spikeekips/mitum/launch/pm"
	"github.com/spikeekips/mitum/launch/process"
	"github.com/spikeekips/mitum/util"
)

var defaultConfigYAML = `
network-id: mitum network; Thu 26 Nov 2020 12:25:18 AM KST
address: node-010a:0.0.1
privatekey: KzmnCUoBrqYbkoP8AUki1AJsyKqxNsiqdrtTB2onyzQfB6MQ5Sef-0112:0.0.1
`

var DefaultConfigVars = kong.Vars{
	"default_config_default_format": "yaml",
}

type DefaultConfigCommand struct {
	*BaseCommand
	Format string `help:"output format, {yaml}" default:"${default_config_default_format}"`
	out    io.Writer
	ctx    context.Context
}

func NewDefaultConfigCommand() DefaultConfigCommand {
	return DefaultConfigCommand{
		BaseCommand: NewBaseCommand("default_config"),
		out:         os.Stdout,
	}
}

func (cmd *DefaultConfigCommand) Run(version util.Version) error {
	if err := cmd.Initialize(cmd, version); err != nil {
		return xerrors.Errorf("failed to initialize command: %w", err)
	}

	if err := cmd.prepare(); err != nil {
		return err
	}

	var conf config.LocalNode
	if err := config.LoadConfigContextValue(cmd.ctx, &conf); err != nil {
		return err
	}

	bconf := config.NewBaseLocalNodePackerYAMLFromConfig(conf)
	bconf.Suffrage = map[string]interface{}{
		"type": "roundrobin",
	}
	bconf.ProposalProcessor = map[string]interface{}{
		"type": "default",
	}

	if b, err := yaml.Marshal(bconf); err != nil {
		return xerrors.Errorf("failed to format config: %w", err)
	} else {
		_, _ = fmt.Fprintln(cmd.out, string(b))
	}

	return nil
}

func (cmd *DefaultConfigCommand) prepare() error {
	switch t := cmd.Format; t {
	case "yaml":
	default:
		return xerrors.Errorf("unknown output format, %q", t)
	}

	ps := pm.NewProcesses()

	if err := process.Config(ps); err != nil {
		return err
	}

	if err := ps.AddProcess(process.ProcessorEncoders, false); err != nil {
		return err
	}

	if err := ps.AddHook(
		pm.HookPrefixPost,
		process.ProcessNameEncoders,
		process.HookNameAddHinters,
		process.HookAddHinters(process.DefaultHinters),
		true,
	); err != nil {
		return err
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, process.ContextValueConfigSource, []byte(defaultConfigYAML))
	ctx = context.WithValue(ctx, process.ContextValueConfigSourceType, "yaml")
	ctx = context.WithValue(ctx, process.ContextValueLog, cmd.Log())
	ctx = context.WithValue(ctx, process.ContextValueVersion, cmd.version)

	_ = ps.SetContext(ctx)

	if err := ps.Run(); err != nil {
		return err
	} else {
		cmd.ctx = ps.Context()

		return nil
	}
}
