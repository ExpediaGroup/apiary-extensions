# Apiary Metastore Events

##  Overview
Apiary Metastore Events contains a set of modules responsible for retrieving and processing Hive Metastore events.

Currently, the following modules are defined:
 - [sns-metastore-events](sns-metastore-events) - SNS Metastore Events contains a set of modules responsible for retrieving and processing Hive Metastore events for SNS.
 - [kafka-metastore-events](kafka-metastore-events) - Kafka Metastore Events contains a set of modules responsible for retrieving and processing Hive Metastore events for Kafka.

## Kafka Configuration

To configure the listener or receiver, either environment variables or Hadoop configuration can be used.

To configure using hadoop, use the configuration below and prefix all parameters with `com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.`.

To configure using environment variables, parameters must all be upper case, use `_` instead of `.`, and be prefixed with `KAFKA` (e.g. `bootstrap.servers` -> `KAKFA_BOOTSTRAP_SERVERS`).

### Kafka Metastore Listener

| Parameter | Required | Default
|:----|:----:|:----:|
| `topic`   | Yes | n/a
| `bootstrap.servers` | Yes | n/a
| `client.id` | Yes | n/a
| `acks` | No | "all"
| `retries` | No | 3
| `max.in.flight.requests.per.connection` | No | 1
| `batch.size` | No | 16384
| `linger.ms` | No | 1
| `buffer.memory` | No | 33554432
| `serde.class` | No | com.expediagroup.apiary.extensions.events.metastore.io.jackson.JsonMetaStoreEventSerDe

Documentation for Kafka Producer configuration can be found [here](https://kafka.apache.org/documentation/#producerconfigs).

### Kafka Metastore Receiver
  
| Parameter | Required | Default
|:----|:----:|:----:| n/a
| `topic`   | Yes | n/a
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

Documentation for Kafka Producer configuration can be found [here](https://kafka.apache.org/documentation/#consumerconfigs).

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
