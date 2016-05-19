package main

import (
	"fmt"
	"os"
	"gopkg.in/Shopify/sarama.v1"
)

var cmdConsumerGroups = &Command{
	Usage: "consumers",
	Short: "list all consumer groups",
	Long: `
Gets a list of all consumer groups from brokers. (0.9.x compatible only)

Example:

    $ k consumers`,
	Run: runConsumerGroups,
}

func runConsumerGroups(cmd *Command, args []string) {
	brokers := brokers()
	config := sarama.NewConfig()
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	
	for _, brokerAddr := range brokers {
		broker := sarama.NewBroker(brokerAddr)
		if err := broker.Open(config); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to broker: %v\n", err)
			continue
		}
		defer broker.Close()
		
		resp, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get consumer groups from broker: %v\n", err)
			continue
		}
		for groupName := range resp.Groups {
			fmt.Printf("%s\n", groupName)
		}
	}
}
