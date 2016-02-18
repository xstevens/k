# k
A general command line client for Apache Kafka.

## Build

```
go build
```

### Cross-compiling

With Go 1.5 or above, cross-compilation support is built-in.
See [Dave Cheney's blog post](http://dave.cheney.net/2015/08/22/cross-compilation-with-go-1-5)
for a tutorial and
the [golang.org docs](https://golang.org/doc/install/source#environment)
for details on `GOOS` and `GOARCH` values for various target operating systems.

For example, to build for Linux on x86:
```
env GOOS=linux GOARCH=386 go build
```

## Usage
```
$ ./k help
Usage: k <command> [options] [arguments]

Environment Variables:
    KAFKA_BROKERS
    SSL_CA_BUNDLE_PATH
    SSL_CRT_PATH
    SSL_KEY_PATH

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
