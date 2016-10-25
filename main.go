package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	app := kingpin.New("k", "A general command line client for Apache Kafka.")
	app.HelpFlag.Short('h')
	app.Version("0.1.0")
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
