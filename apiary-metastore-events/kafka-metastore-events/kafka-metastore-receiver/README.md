# Apiary Kafka Metastore Receiver

## Overview

This module contains classes for retrieving and processing Hive Metastore events from Kafka.

## Kafa Metastore Receiver Configuration

To configure the receiver, a builder is provided. The builder requires that bootstrap servers, a topic name and an application name are specified.

The receiver uses the application name to create a unique group id for each consumer group: `apiary-kafka-metastore-receiver-<app name>`.

```
Properties additionalProperties = new Properties();
additionalProperties.put(key, value);
KafkaMessageReader reader = KafkaMessageReader.Builder.aKafkaMessageReader(bootstapServers, topicName, applicationName)
  .withConsumerProperties(additionalProperties)
  .build();
```

Additional properties to configure the Kafka consumer may be configured too, please see documentation for more details on what configuration is available [here](https://kafka.apache.org/documentation/#consumerconfigs).

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
