package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

// based off of kingpin.SeparateOptionalFlagsUsageTemplate but added
// a section on environment variables
var FlagsUsageTemplate = `{{define "FormatCommand"}}\
{{if .FlagSummary}} {{.FlagSummary}}{{end}}\
{{range .Args}} {{if not .Required}}[{{end}}<{{.Name}}>{{if .Value|IsCumulative}}...{{end}}{{if not .Required}}]{{end}}{{end}}\
{{end}}\

{{define "FormatCommands"}}\
{{range .FlattenedCommands}}\
{{if not .Hidden}}\
  {{.FullCommand}}{{if .Default}}*{{end}}{{template "FormatCommand" .}}
{{.Help|Wrap 4}}
{{end}}\
{{end}}\
{{end}}\

{{define "FormatUsage"}}\
{{template "FormatCommand" .}}{{if .Commands}} <command> [<args> ...]{{end}}
{{if .Help}}
{{.Help|Wrap 0}}\
{{end}}\

{{end}}\
{{if .Context.SelectedCommand}}\
usage: {{.App.Name}} {{.Context.SelectedCommand}}{{template "FormatUsage" .Context.SelectedCommand}}
{{else}}\
usage: {{.App.Name}}{{template "FormatUsage" .App}}
{{end}}\

Environment variables:
    KAFKA_BROKERS or TLS_KAFKA_BROKERS
    SSL_CA_BUNDLE_PATH
    SSL_CRT_PATH
    SSL_KEY_PATH
	
{{if .Context.Flags|RequiredFlags}}\
Required flags:
{{.Context.Flags|RequiredFlags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if  .Context.Flags|OptionalFlags}}\
Optional flags:
{{.Context.Flags|OptionalFlags|FlagsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.Args}}\
Args:
{{.Context.Args|ArgsToTwoColumns|FormatTwoColumns}}
{{end}}\
{{if .Context.SelectedCommand}}\
Subcommands:
{{if .Context.SelectedCommand.Commands}}\
{{template "FormatCommands" .Context.SelectedCommand}}
{{end}}\
{{else if .App.Commands}}\
Commands:
{{template "FormatCommands" .App}}
{{end}}\
`

func main() {
	app := kingpin.New("k", "A general command line client for Apache Kafka.")
	app.HelpFlag.Short('h')
	app.Version("0.1.0")
	app.UsageTemplate(FlagsUsageTemplate)
	configureConsumeCommand(app)
	configureConsumerGroupsCommand(app)
	configureOffsetsCommand(app)
	configureProduceCommand(app)
	configureTLSCommand(app)
	configureTopicsCommand(app)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func must(err error) {
	if err != nil {
		errString := strings.TrimPrefix(err.Error(), "k: ")
		fmt.Fprintf(os.Stderr, "error: %s\n", errString)
		os.Exit(1)
	}
}
