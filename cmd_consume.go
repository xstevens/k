package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"

	"gopkg.in/Shopify/sarama.v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

type ConsumeCommand struct {
	ApiVersion string
	Topic      string
	Partition  int32
	Offset     int64
	N          int
}

func configureConsumeCommand(app *kingpin.Application) {
	cc := &ConsumeCommand{}
	consume := app.Command("consume", "Consumes messages from the given topic and writes them to stdout.").Action(cc.runConsume)
	consume.Flag("apiversion", "the Kafka API version to use").StringVar(&cc.ApiVersion)
	consume.Flag("topic", "topic to consume").Short('t').Required().StringVar(&cc.Topic)
	consume.Flag("partition", "partition to consume").Short('p').Default("-1").Int32Var(&cc.Partition)
	consume.Flag("offset", "starting offset for consumer").Short('o').Default("0").Int64Var(&cc.Offset)
	consume.Flag("nmessages", "number of messages to consume").Short('n').Default("-1").IntVar(&cc.N)
}

func (cc *ConsumeCommand) runConsume(ctx *kingpin.ParseContext) error {
	config := sarama.NewConfig()
	config.Version = getKafkaVersion(cc.ApiVersion)
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	brokers := brokers(useTLS)
	config.Net.TLS.Enable = useTLS
	config.Net.TLS.Config = tlsConfig
	config.ClientID = "k-consume"
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
	if cc.Partition < 0 {
		partitions, err = client.Partitions(cc.Topic)
		must(err)
	} else {
		partitions = make([]int32, 1)
		partitions[0] = cc.Partition
	}

	for _, part := range partitions {
		// calculate a starting offset
		oldestOffset, newestOffset := offsets(client, cc.Topic, part)
		must(err)
		startingOffset := newestOffset
		// if offset is less than zero we're going to rewind from newest
		// if offset is greater than zero we'll use the offset as is as long as it
		// is in range
		offsetsDelta := newestOffset - oldestOffset
		if cc.Offset < 0 && offsetsDelta > 0 {
			startingOffset = newestOffset + cc.Offset
		} else if cc.Offset > 0 && cc.Offset >= oldestOffset && cc.Offset < newestOffset {
			startingOffset = cc.Offset
		}

		fmt.Fprintf(os.Stderr, "Partition: %d, using starting offset: %d\n", part, startingOffset)
		partConsumer, err := consumerLeader.ConsumePartition(cc.Topic, part, startingOffset)
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
				if cc.N > 0 && received >= cc.N {
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

	return nil
}
