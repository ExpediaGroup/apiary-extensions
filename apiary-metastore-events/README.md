# Apiary Metastore Events

##  Overview
The Apiary Metastore Events contains set of modules responsible for retrieving and processing of the Hive Metastore events.

Currently, the following modules are defined:
 - [apiary-metastore-listener](apiary-metastore-listener) - Listens to the events from Hive Metastore and publishes them to SNS
 - [apiary-receivers](apiary-receivers) - Provides an implementation for polling SQS messages
 
The architecture below represents the flow of processing Hive Metastore Events:

![Apiary SNS event listener Architecture.](images/Apiary_SNS_event_listener.png  "Apiary SNS event listener.")

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
