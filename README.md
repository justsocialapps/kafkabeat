# Kafkabeat

[![Build Status](https://travis-ci.org/justsocialapps/kafkabeat.svg?branch=master)](https://travis-ci.org/justsocialapps/kafkabeat)

Kafkabeat is an elastic [Beat](https://www.elastic.co/products/beats) that reads
events from one or more [Kafka](https://kafka.apache.org) topics and forwards them to
[Logstash](https://www.elastic.co/products/logstash) (or any other configured output).

The Kafka consumer implements an at-least-once behaviour which means that
messages may be forwarded to the configured output more than once.

## Getting Started with Kafkabeat

### Requirements

* [Golang](https://golang.org/dl/) 1.7

### Building

```sh
# Make sure $GOPATH is set
go get github.com/justsocialapps/kafkabeat
cd $GOPATH/src/github.com/justsocialapps/kafkabeat
make
```

### Running

To run Kafkabeat with debugging output enabled, run:

```sh
./kafkabeat -c kafkabeat.yml -e -d "*"
```

### Configuring

An example configuration can be found in the file `kafkabeat.yml`. The following
parameters are specific to Kafkabeat:

```yaml
kafkabeat:
    # a list of Kafka brokers to connect to
    brokers: ["localhost:9092"]

    # A list of topics to subscribe to
    topics: ["tracking"]

    # The consumer group to join
    group: "kafkabeat"

    # Codec used to deserialize kafka event, "plain" or "json".
    # Defaults to plain.
    #codec: "plain"

    # Kafka Offset handling, "newest" or "oldest".
    # Defaults to "newest"
    #offset: "newest"
```

### Testing

To test Kafkabeat, run the following command:

```sh
make testsuite
```

alternatively:

```sh
make unit-tests
make system-tests
make integration-tests
make coverage-report
```

The test coverage is reported in the folder `./build/coverage/`
