package main

import (
	"os"
	"strings"

	"gopkg.in/Shopify/sarama.v1"
)

func brokers() []string {
	s := os.Getenv("KAFKA_BROKERS")
	if s == "" {
		s = "127.0.0.1:9092"
	}
	return strings.Split(s, ",")
}

func offsets(client sarama.Client, topic string, partition int32) (oldest int64, newest int64) {
	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	must(err)
	newest, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	must(err)
	return oldest, newest
}
