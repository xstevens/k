package main

import (
	"fmt"

	"gopkg.in/Shopify/sarama.v1"
)

var cmdOffsets = &Command{
	Usage: "offsets --topic <topic>",
	Short: "show the oldest and newest offset for a given topic and partition",
	Long: `
Prints oldest and newest offsets for the given topic to stdout.

Example:

    $ k offsets --topic foo`,
	Run: runOffsets,
}

func runOffsets(cmd *Command, args []string) {
	config := sarama.NewConfig()
	config.Version = kafkaVersion
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
	parts, err := client.Partitions(topic)
	must(err)

	// print offsets for each partition
	for _, part := range parts {
		oldestOffset, newestOffset := offsets(client, topic, part)
		fmt.Printf("partition=%d oldest=%d newest=%d\n", part, oldestOffset, newestOffset)
	}
}

func init() {
	cmdOffsets.Flag.StringVarP(&topic, "topic", "t", "k", "get offsets for topic")
}
