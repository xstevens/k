package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"gopkg.in/Shopify/sarama.v1"
)

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
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "k consume"
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient(brokers, config)
	must(err)
	defer client.Close()

	consumerLeader, err := sarama.NewConsumerFromClient(client)
	must(err)
	defer consumerLeader.Close()

	var (
		messages = make(chan *sarama.ConsumerMessage, 10000)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		defer close(signals)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		fmt.Fprintln(os.Stderr, "Initiating shutdown of consumers ...")
		close(closing)
	}()

	// get the list of partitions unless a specific one was specified
	var partitions []int32
	if partition < 0 {
		partitions, err = client.Partitions(topic)
		must(err)
	} else {
		partitions = make([]int32, 1)
		partitions[0] = partition
	}

	for _, part := range partitions {
		// calculate a starting offset
		oldestOffset, newestOffset := offsets(client, topic, part)
		must(err)
		startingOffset := newestOffset
		// if offset is less than zero we're going to rewind from newest
		// if offset is greater than zero we'll use the offset as is as long as it
		// is in range
		offsetsDelta := newestOffset - oldestOffset
		if offset < 0 && offsetsDelta > 0 {
			startingOffset = newestOffset + offset
		} else if offset > 0 && offset >= oldestOffset && offset < newestOffset {
			startingOffset = offset
		}

		fmt.Fprintf(os.Stderr, "Partition: %d, using starting offset: %d\n", part, startingOffset)
		partConsumer, err := consumerLeader.ConsumePartition(topic, part, startingOffset)
		must(err)

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(partConsumer)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
		consumerLoop:
			for {
				select {
				case err := <-pc.Errors():
					fmt.Fprintf(os.Stderr, "Failed to receive message: %s\n", err)
				case message := <-pc.Messages():
					messages <- message
				case <-closing:
					break consumerLoop
				}
			}
		}(partConsumer)
	}

	go func() {
		var received, errors int
	printLoop:
		for {
			select {
			case msg := <-messages:
				if msg.Key != nil {
					fmt.Printf("%s\t", string(msg.Key))
				}
				fmt.Printf("%s\n", string(msg.Value))
				received++
				if n > 0 && received >= n {
					close(closing)
				}
			case <-closing:
				break printLoop
			}
		}
		fmt.Fprintf(os.Stderr, "Messages received: %d, errors: %d\n", received, errors)
	}()

	wg.Wait()
	fmt.Fprintln(os.Stderr, "Consumers finished.")
	close(messages)
	fmt.Fprintln(os.Stderr, "Exiting.")
}

func init() {
	cmdConsume.Flag.StringVarP(&topic, "topic", "t", "", "topic to consume")
	cmdConsume.Flag.Int32VarP(&partition, "partition", "p", -1, "partition to consume")
	cmdConsume.Flag.Int64VarP(&offset, "offset", "o", 0, "starting offset for consumer")
	cmdConsume.Flag.IntVarP(&n, "n", "n", -1, "number of messages to consume")
}
