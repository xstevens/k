# k
A general command line client for Apache Kafka.

## Build

```
go build
```

### Cross-compiling

I typically use gox for cross-compiling. Once it's installed it's really easy to use.

```
gox -osarch="linux/amd64"
```

## Usage
```
$ ./k help
Usage: k <command> [options] [arguments]

Environment Variables:
    KAFKA_BROKERS

Commands:
    produce     produce messages to given topic
    consume     consume messages from given topic
    offsets     show the oldest and newest offset for a given topic and partition
    topics      show the list of topics
    help        show help

Run 'k help <command>' for details.
```

## License
All aspects of this software are distributed under the MIT License. See LICENSE file for full license text.

## Inspirations
Heavily inspired (grabbed basis of code from) [Mark McGranaghan's zk CLI for Zookeeper](https://github.com/mmcgrana/zk).
