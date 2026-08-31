# Apiary Kafka Metastore Receiver

## Overview

This module contains classes for retrieving and processing Hive Metastore events from Kafka.

## Kafa Metastore Receiver Configuration

To configure the receiver, a builder is provided. The builder requires that bootstrap servers, a topic name and an application name are specified.

The receiver uses the application name to create a unique group id for each consumer group: `apiary-kafka-metastore-receiver-<app name>`.

```
Properties additionalProperties = new Properties();
additionalProperties.put(key, value);
KafkaMessageReader reader = KafkaMessageReaderBuilder.builder(bootstapServers, topicName, applicationName)
  .withConsumerProperties(additionalProperties)
  .build();
```

Additional properties to configure the Kafka consumer may be configured too, please see documentation for more details on what configuration is available [here](https://kafka.apache.org/documentation/#consumerconfigs).

Note that additional properties do not override the values the builder sets itself (bootstrap servers, group id, and the key and value deserializers). Where an override is supported, a dedicated builder method is provided.

## Key deserializer

By default the receiver reads record keys with a `LongDeserializer`, matching the key written by the Apiary Hive Metastore listener.

Topics populated by a different producer may key their records with another type. Reading those with the default deserializer fails with `SerializationException: Size of data received by LongDeserializer is not 8`, and because the consumer position does not advance past a record it cannot deserialize, the reader makes no progress. Set the deserializer that matches the producer:

```
KafkaMessageReader reader = KafkaMessageReaderBuilder.builder(bootstapServers, topicName, applicationName)
  .withKeyDeserializer(StringDeserializer.class.getName())
  .build();
```

The record key is never used to decode the event — events are read from the record value alone — so any deserializer that can read the key is safe here.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
