# Apiary Kafka Metastore Events

## Overview
Apiary Metastore Events contains a set of modules responsible for retrieving and processing Hive Metastore events.

Currently, the following modules are defined:
 - [kafka-metastore-integration-tests](kafka-metastore-integration-tests) - Contains integration tests for Kafka Metastore Listener and Receiver.
 - [kafka-metastore-listener](kafka-metastore-listener) - Contains a set of modules responsible for publishing Hive Metastore events to Kafka.
 - [kafka-metastore-receiver](kafka-metastore-receiver) - Contains a set of modules responsible for retrieving and processing Hive Metastore events from Kafka.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2020 Expedia, Inc.
