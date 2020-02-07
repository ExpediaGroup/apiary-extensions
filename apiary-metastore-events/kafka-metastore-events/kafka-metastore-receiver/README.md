# Apiary Kafka Metastore Receiver

## Overview

This module contains classes for retrieving and processing Hive Metastore events from Kafka.

## Kafa Metastore Receiver Configuration

To configure the receiver, a builder is provided. The following parameters are available:
  
| Parameter | Required | Default
|:----|:----:|:----:|
| `topic.name`   | Yes | n/a
| `bootstrap.servers` | Yes | n/a
| `group.id` | Yes | n/a
| `client.id` | Yes | n/a
| `session.timeout.ms` | No | 30000
| `connections.max.idle.ms` | No | 9
| `reconnect.backoff.max.ms` | No | 1
| `reconnect.backoff.ms` | No | 50
| `retry.backoff.ms` | No | 100
| `max.poll.interval.ms` | No | 300000
| `max.poll.records` | No | 500
| `enable.auto.commit` | No | "true"
| `auto.commit.interval.ms` | No | 5000
| `fetch.max.bytes` | No | 52428800
| `receive.buffer.bytes` | No | 65536

For more information about these parameters, documentation for Kafka Producer configuration can be found [here](https://kafka.apache.org/documentation/#consumerconfigs).

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
