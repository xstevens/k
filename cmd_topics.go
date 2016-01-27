package main

import (
	"fmt"

	"gopkg.in/Shopify/sarama.v1"
)

var cmdTopics = &Command{
	Usage: "topics",
	Short: "show the list of topics",
	Long: `
Prints the list of topics to stdout.

Example:

    $ k topics`,
	Run: runTopics,
}

func runTopics(cmd *Command, args []string) {
	brokers := brokers()
	config := sarama.NewConfig()
	config.ClientID = "k topics"
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	// get list of topics
	topics, err := client.Topics()
	must(err)
	for _, topic := range topics {
		fmt.Println(topic)
	}
}
