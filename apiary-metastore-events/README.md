# Apiary Metastore Events

##  Overview
Apiary Metastore Events contains a set of modules responsible for retrieving and processing Hive Metastore events.

Currently, the following modules are defined:
 - [apiary-metastore-listener](apiary-metastore-listener) - listens to events from the Hive Metastore and publishes them to an SNS topic.
 - [apiary-receivers](apiary-receivers) - common logic to simplify receiving and deserialising Hive Metastore event messages published by the `apiary-metastore-listener`.
 - [apiary-metastore-consumers](apiary-metastore-consumers) - consumers that receive and process Hive Metastore event messages published by the `apiary-metastore-listener`.
  
The architecture below represents the flow of processing Hive Metastore events:

![Apiary Metastore Events Architecture.](images/Apiary_metastore_events_architecture.png  "Apiary SNS event listener.")

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
