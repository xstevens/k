package main

import (
	"bufio"
	"fmt"
	"github.com/shopify/sarama"
	"os"
	"os/signal"
	"strings"
	"sync"
)

func brokers() []string {
	s := os.Getenv("KAFKA_BROKERS")
	if s == "" {
		s = "127.0.0.1:9092"
	}
	return strings.Split(s, ",")
}

var (
	topic string
)

var cmdProduce = &Command{
	Usage: "produce --topic <topic>",
	Short: "produce messages to given topic",
	Long: `
Produces a message to the given topic with the data given by
reading stdin (newline delimited).

Example:

    $ echo content | k produce --topic foo`,
	Run: runProduce,
}

func runProduce(cmd *Command, args []string) {
	brokers := brokers()
	config := sarama.NewConfig()
	config.ClientID = "k produce"
	config.Producer.Return.Successes = true
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	producer, err := sarama.NewAsyncProducerFromClient(client)
	must(err)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	defer close(signals)

	var wg sync.WaitGroup
	var enqueued, successes, errors int

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _ = range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			fmt.Fprintf(os.Stderr, "Failed to produce message: %s\n", err)
			errors++
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
producerLoop:
	for scanner.Scan() {
		msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(scanner.Bytes())}
		select {
		case producer.Input() <- msg:
			enqueued++
		case <-signals:
			break producerLoop
		}
	}

	producer.AsyncClose()
	wg.Wait()
	fmt.Fprintf(os.Stderr, "messages produced: %d, errors: %d\n", successes, errors)
}

var cmdConsume = &Command{
	Usage: "consume --topic <topic>",
	Short: "consume messages from given topic",
	Long: `
Consumes messages from the given topic and writes them to stdout.

Example:

    $ k consume --topic foo`,
	Run: runConsume,
}

func runConsume(cmd *Command, args []string) {
	brokers := brokers()
	config := sarama.NewConfig()
	config.ClientID = "k produce"
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	// parts, err := client.Partitions(topic)
	// must(err)

	leader, err := sarama.NewConsumerFromClient(client)
	must(err)
	defer leader.Close()

	signals := make(chan os.Signal, 1)
	defer close(signals)
	signal.Notify(signals, os.Interrupt)

	// TODO: support specifying offsets and relative offsets
	//latestOffset, err := client.GetOffset(topic, 0, sarama.OffsetNewest)
	earliestOffset, err := client.GetOffset(topic, 0, sarama.OffsetOldest)
	must(err)
	startingOffset := earliestOffset

	// TODO: support consuming all partitions
	consumer, err := leader.ConsumePartition(topic, 0, startingOffset)
	must(err)
	// for _, part := range parts {
	// 	latestOffset, err := client.GetOffset(*topic, 0, sarama.OffsetNewest)
	// 	if err == nil {
	// 		consumer, err := leader.ConsumePartition(*topic, part, latestOffset)
	// 		must(err)
	// 	}
	// }

	var received, errors int
consumerLoop:
	for {
		select {
		case msg := <-consumer.Messages():
			fmt.Println(string(msg.Value))
			received++
		case err := <-consumer.Errors():
			fmt.Fprintf(os.Stderr, "Failed to receive message: %s\n", err)
			errors++
		case <-signals:
			break consumerLoop
		}
	}

	fmt.Fprintf(os.Stderr, "messages received: %d, errors: %d\n", received, errors)
}

func init() {
	cmdProduce.Flag.StringVarP(&topic, "topic", "t", "k", "topic")
	cmdConsume.Flag.StringVarP(&topic, "topic", "t", "k", "topic")
}
