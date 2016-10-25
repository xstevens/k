# k
A general command line client for Apache Kafka.

## Install

```
# Set a directory where Go packages will be installed.
export GOPATH=$HOME/go

# Install `k` and dependencies.
go get github.com/xstevens/k
```

This will make `k` available as `$GOPATH/bin/k`.
Add `$GOPATH/bin` to your `$PATH` to have it available as `k`.

### Build

To run subsequent builds, use `go build`:

```
# Ensure you're in the `k` source directory.
cd $GOPATH/src/github.com/xstevens/k

# Run the build.
go build
```

### Cross-compiling

With Go 1.5 or above, cross-compilation support is built in.
See [Dave Cheney's blog post](http://dave.cheney.net/2015/08/22/cross-compilation-with-go-1-5)
for a tutorial and the [golang.org docs](https://golang.org/doc/install/source#environment)
for details on `GOOS` and `GOARCH` values for various target operating systems.

A typical build for Linux would be:
```
# Ensure you're in the `k` source directory.
cd $GOPATH/src/github.com/xstevens/k

# Run the build.
GOOS=linux GOARCH=amd64 go build
```

## Usage
```
$ ./k
usage: k [<flags>] <command> [<args> ...]

A general command line client for Apache Kafka.

Environment variables:
    KAFKA_BROKERS or TLS_KAFKA_BROKERS
    SSL_CA_BUNDLE_PATH
    SSL_CRT_PATH
    SSL_KEY_PATH

Optional flags:
  -h, --help     Show context-sensitive help (also try --help-long and --help-man).
      --version  Show application version.

Commands:
  help [<command>...]
    Show help.

  consume --topic=TOPIC [<flags>]
    Consumes messages from the given topic and writes them to stdout.

  consumers [<flags>]
    Gets a list of all consumer groups from brokers. (0.9+ compatible only)

  offsets --topic=TOPIC [<flags>]
    Prints oldest and newest offsets for the given topic to stdout.

  produce --topic=TOPIC [<flags>]
    Produces a message to the given topic with the data given by reading stdin (newline delimited).

    If the message includes a tab character, the content before the tab will be interpreted as the message's key.

  tls
    Connects to broker using Transport Layer Security which can be useful for TLS handshake debugging.

  topics [<flags>]
    Prints the list of topics to stdout.
```

## License
All aspects of this software are distributed under the MIT License. See LICENSE file for full license text.

## Inspirations
Originally inspired by [Mark McGranaghan's zk CLI for Zookeeper](https://github.com/mmcgrana/zk).
