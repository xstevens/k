package main

import (
	"fmt"

	"gopkg.in/Shopify/sarama.v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

type OffsetsCommand struct {
	ApiVersion string
	Topic      string
}

func configureOffsetsCommand(app *kingpin.Application) {
	oc := &OffsetsCommand{}
	offsets := app.Command("offsets", "Prints oldest and newest offsets for the given topic to stdout.").Action(oc.runOffsets)
	offsets.Flag("apiversion", "the Kafka API version to use").StringVar(&oc.ApiVersion)
	offsets.Flag("topic", "get offsets for topic").Short('t').Required().StringVar(&oc.Topic)
}

func (oc *OffsetsCommand) runOffsets(ctx *kingpin.ParseContext) error {
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(oc.ApiVersion)
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	brokers := brokers(useTLS)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "k-offsets"
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	// get partitions for topic
	parts, err := client.Partitions(oc.Topic)
	must(err)

	// print offsets for each partition
	for _, part := range parts {
		oldestOffset, newestOffset := offsets(client, oc.Topic, part)
		fmt.Printf("partition=%d oldest=%d newest=%d\n", part, oldestOffset, newestOffset)
	}

	return nil
}
