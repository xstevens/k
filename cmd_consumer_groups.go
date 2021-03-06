package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/Shopify/sarama.v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

type ConsumerGroupsCommand struct {
	ApiVersion string
}

func configureConsumerGroupsCommand(app *kingpin.Application) {
	cgc := &ConsumerGroupsCommand{}
	consumerGroups := app.Command("consumers", "Gets a list of all consumer groups from brokers. (0.9+ compatible only)").Action(cgc.runConsumerGroups)
	consumerGroups.Flag("apiversion", "the Kafka API version to use").StringVar(&cgc.ApiVersion)
}

func (cgc *ConsumerGroupsCommand) runConsumerGroups(ctx *kingpin.ParseContext) error {
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(cgc.ApiVersion)
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	brokers := brokers(useTLS)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "k-consumer-groups"
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	topicPartitions := make(map[string][]int32)
	topics, err := client.Topics()
	must(err)
	for _, topic := range topics {
		parts, err := client.Partitions(topic)
		must(err)
		topicPartitions[topic] = parts
	}

	for _, brokerAddr := range brokers {
		broker := sarama.NewBroker(brokerAddr)
		if err := broker.Open(config); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to broker: %v\n", err)
			continue
		}
		defer broker.Close()

		listGroupResp, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get consumer groups from broker: %v\n", err)
			continue
		}
		for groupName := range listGroupResp.Groups {
			descGroupReq := &sarama.DescribeGroupsRequest{
				Groups: []string{groupName},
			}
			descGroupResp, err := broker.DescribeGroups(descGroupReq)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to describe consumer groups: %v\n", err)
				continue
			}
			for _, groupDesc := range descGroupResp.Groups {

				var members []string
				for _, memberDesc := range groupDesc.Members {
					members = append(members, fmt.Sprintf("%s%s", memberDesc.ClientId, memberDesc.ClientHost))
				}
				fmt.Printf("group.id=%s protocol=%s protocol.type=%s state=%s members=%s ",
					groupDesc.GroupId, groupDesc.Protocol, groupDesc.ProtocolType, groupDesc.State,
					strings.Join(members, ","))

				consumerMetaReq := &sarama.ConsumerMetadataRequest{
					ConsumerGroup: groupDesc.GroupId,
				}
				consumerMetaResp, err := broker.GetConsumerMetadata(consumerMetaReq)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to get consumer group metadata: %v\n", err)
					continue
				}
				fmt.Printf("coordinator=%d/%s:%d\n", consumerMetaResp.CoordinatorID, consumerMetaResp.CoordinatorHost, consumerMetaResp.CoordinatorPort)
			}
		}
	}

	return nil
}
