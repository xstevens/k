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

func offsets(client sarama.Client, topic string, partition int32) (oldest int64, newest int64) {
	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	must(err)
	newest, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	must(err)
	return oldest, newest
}

var (
	topic     string
	partition int32
	offset    int64
	n         int
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
	Usage: "consume --topic <topic> [--partition <partition>] [--offset <offset>] [-n <n_messages>]",
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
	config.ClientID = "k consume"
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	must(err)
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	defer close(signals)
	signal.Notify(signals, os.Interrupt)

	// calculate a starting offset
	_, newestOffset := offsets(client, topic, partition)
	must(err)
	startingOffset := newestOffset
	if offset < 0 {
		startingOffset = newestOffset + offset
	} else if offset > 0 {
		startingOffset = offset
	}

	// TODO: support consuming all partitions
	fmt.Fprintf(os.Stderr, "Using starting offset: %d\n", startingOffset)
	partConsumer, err := consumer.ConsumePartition(topic, partition, startingOffset)
	must(err)
	defer partConsumer.Close()

	var received, errors int
consumerLoop:
	for {
		select {
		case msg := <-partConsumer.Messages():
			fmt.Println(string(msg.Value))
			received++
			if n > 0 && received >= n {
				break consumerLoop
			}
		case err := <-partConsumer.Errors():
			fmt.Fprintf(os.Stderr, "Failed to receive message: %s\n", err)
			errors++
		case <-signals:
			break consumerLoop
		}
	}

	fmt.Fprintf(os.Stderr, "Messages received: %d, errors: %d\n", received, errors)
}

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
	brokers := brokers()
	config := sarama.NewConfig()
	config.ClientID = "k offsets"
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	// get partitions for topic
	parts, err := client.Partitions(topic)
	must(err)

	// print offsets for each partition
	for _, part := range parts {
		oldestOffset, newestOffset := offsets(client, topic, 0)
		must(err)
		fmt.Printf("partition=%d oldest=%d newest=%d\n", part, oldestOffset, newestOffset)
	}
}

func init() {
	cmdProduce.Flag.StringVarP(&topic, "topic", "t", "k", "produce to topic")
	cmdConsume.Flag.StringVarP(&topic, "topic", "t", "k", "topic to consume")
	cmdConsume.Flag.Int32VarP(&partition, "partition", "p", 0, "partition to consume")
	cmdConsume.Flag.Int64VarP(&offset, "offset", "o", 0, "starting offset for consumer")
	cmdConsume.Flag.IntVarP(&n, "n", "n", -1, "number of messages to consume")
	cmdOffsets.Flag.StringVarP(&topic, "topic", "t", "k", "get offsets for topic")
}
