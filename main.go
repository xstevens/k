package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"gopkg.in/Shopify/sarama.v1"
)

type Command struct {
	Usage string
	Flag  pflag.FlagSet
	Short string
	Long  string
	Run   func(cmd *Command, args []string)
}

func (c *Command) Name() string {
	name := c.Usage
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func printOverviewUsage(w io.Writer) {
	fmt.Fprintf(w, "Usage: k <command> [options] [arguments]\n")
	fmt.Fprintf(w, "\nEnvironment Variables: \n")
	fmt.Fprintf(w, "    KAFKA_BROKERS or TLS_KAFKA_BROKERS\n")
	fmt.Fprintf(w, "    SSL_CA_BUNDLE_PATH\n")
	fmt.Fprintf(w, "    SSL_CRT_PATH\n")
	fmt.Fprintf(w, "    SSL_KEY_PATH\n")
	fmt.Fprintf(w, "\nCommands:\n")
	for _, command := range commands {
		fmt.Fprintf(w, "    %-8s    %s\n", command.Name(), command.Short)
	}
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "Run 'k help <command>' for details.\n")
}

func printLongUsage(c *Command) {
	fmt.Printf("Usage: k %s\n\n", c.Usage)
	fmt.Println(strings.Trim(c.Long, "\n"))
}

func printAbbrevUsage(c *Command) {
	fmt.Fprintf(os.Stderr, "Usage: k %s\n", c.Usage)
	fmt.Fprintf(os.Stderr, "Run 'k help %s' for details.\n", c.Name())
}

var cmdHelp = &Command{
	Usage: "help [<command>]",
	Short: "show help",
	Long: `
Help shows usage details for a single command if one is given, or
overview usage if no command is specified.`,
	Run: runHelp,
}

func runHelp(cmd *Command, args []string) {
	if len(args) > 1 {
		fmt.Fprintf(os.Stderr, "error: too many arguments")
		os.Exit(2)
	}
	if len(args) == 0 {
		printOverviewUsage(os.Stdout)
		return
	}
	for _, c := range commands {
		if c.Name() == args[0] {
			printLongUsage(c)
			return
		}
	}
	fmt.Fprintf(os.Stderr, "error: unrecognized command: %s\n", args[0])
	fmt.Fprintf(os.Stderr, "Run 'k help' for usage.\n")
	os.Exit(2)
}

var (
	commands     []*Command
	source       string
	topic        string
	partition    int32
	offset       int64
	n            int
	version      string
	kafkaVersion sarama.KafkaVersion
)

func init() {
	commands = []*Command{
		cmdProduce,
		cmdConsume,
		cmdConsumerGroups,
		cmdOffsets,
		cmdTopics,
		cmdTLS,
		cmdHelp,
	}
	kafkaVersion = getKafkaVersion(version)
}

func main() {
	args := os.Args[1:]
	if len(args) < 1 || strings.IndexRune(args[0], '-') == 0 {
		printOverviewUsage(os.Stderr)
		os.Exit(2)
	}

	for _, cmd := range commands {
		if cmd.Name() == args[0] {
			cmd.Flag.SetInterspersed(true)
			cmd.Flag.Usage = func() {
				printAbbrevUsage(cmd)
			}
			if err := cmd.Flag.Parse(args[1:]); err == pflag.ErrHelp {
				cmdHelp.Run(cmdHelp, args[:1])
				return
			} else if err != nil {
				os.Exit(2)
			}
			cmd.Run(cmd, cmd.Flag.Args())
			return
		}
	}

	fmt.Fprintf(os.Stderr, "error: unrecognized command: %s\n", args[0])
	fmt.Fprintf(os.Stderr, "Run 'k help' for usage.\n")
	os.Exit(2)
}

func failUsage(cmd *Command) {
	printAbbrevUsage(cmd)
	os.Exit(2)
}

func must(err error) {
	if err != nil {
		errString := strings.TrimPrefix(err.Error(), "k: ")
		fmt.Fprintf(os.Stderr, "error: %s\n", errString)
		os.Exit(1)
	}
}

func inData() []byte {
	data, _ := ioutil.ReadAll(os.Stdin)
	return data
}

func outString(p string, args ...interface{}) {
	fmt.Fprintf(os.Stdout, p, args...)
}

func outData(d []byte) {
	os.Stdout.Write(d)
}
