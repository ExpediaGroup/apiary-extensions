# Overview

Consumers receive and process Hive Metastore event messages published by the `apiary-metastore-listener`. Each of the consumers has its own SQS Queue. SQS Queue subscribes to relevant events from an SNS topic.
 
## Supported Apiary Hive Consumers

|Consumer Name|Connection to SQS| Subscribed Event Types | Description
|----|----|----|----
|PrivilegesGrantor|Lambda|CREATE_TABLE|Grants Public Role Privileges to a newly created table 
