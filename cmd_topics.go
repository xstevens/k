package main

import (
	"fmt"
	"sort"

	"gopkg.in/Shopify/sarama.v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

type TopicsCommand struct {
	ApiVersion string
}

func configureTopicsCommand(app *kingpin.Application) {
	tc := &TopicsCommand{}
	topics := app.Command("topics", "Prints the list of topics to stdout.").Action(tc.runTopics)
	topics.Flag("apiversion", "the Kafka API version to use").StringVar(&tc.ApiVersion)
}

func (tc *TopicsCommand) runTopics(ctx *kingpin.ParseContext) error {
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(tc.ApiVersion)
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	brokers := brokers(useTLS)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "k-topics"
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	// get list of topics
	topics, err := client.Topics()
	must(err)
	sort.Strings(topics)
	for _, topic := range topics {
		fmt.Println(topic)
	}

	return nil
}
