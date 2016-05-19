package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"gopkg.in/Shopify/sarama.v1"
)

var cmdProduce = &Command{
	Usage: "produce --topic <topic>",
	Short: "produce messages to given topic",
	Long: `
Produces a message to the given topic with the data given by
reading stdin (newline delimited).

If the message includes a tab character, the content before the
tab will be interpreted as the message's key.

Example:

    $ echo content | k produce --topic foo`,
	Run: runProduce,
}

func runProduce(cmd *Command, args []string) {
	brokers := brokers()
	config := sarama.NewConfig()
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "k-produce"
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
		line := scanner.Text()
		idx := strings.Index(line, "\t")
		var msg *sarama.ProducerMessage
		if idx > 0 {
			msg = &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(line[0:idx]), Value: sarama.ByteEncoder(line[idx+1:])}
		} else {
			msg = &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(line)}
		}
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

func init() {
	cmdProduce.Flag.StringVarP(&topic, "topic", "t", "", "produce to topic")
}
