# Overview

Consumers receive and process Hive Metastore event messages published by the `apiary-metastore-listener`. Each of the consumers has its own SQS Queue which subscribes to relevant events from a SNS topic.
 
## Supported Apiary Hive Consumers

|Consumer Name|Connection to SQS| Subscribed Event Types | Description
|----|----|----|----
|PrivilegesGrantor|Lambda|CREATE_TABLE|Grants Public Role Privileges to a newly created table 

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
