# Apiary Metastore Events

##  Overview
Apiary Metastore Events contains set of modules responsible for retrieving and processing of the Hive Metastore events.

Currently, the following modules are defined:
 - [apiary-metastore-listener](apiary-metastore-listener) - listens to events from Hive Metastore and publishes them to SNS
 - [apiary-receivers](apiary-receivers) - provides an implementation for receiving and deserializing Hive events from a queue
 
The architecture below represents the flow of processing Hive Metastore events:

![Apiary SNS event listener Architecture.](images/Apiary_SNS_event_listener.png  "Apiary SNS event listener.")

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
