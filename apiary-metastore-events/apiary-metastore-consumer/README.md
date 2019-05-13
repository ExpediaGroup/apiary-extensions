# Overview

`apiary-hive-consumers` provides implementation of features that are relying on the Hive Metastore events.
  
 Each of the consumers consists of:
  - SQS Queue (retrieves/filters messages from SNS)
  - SQS Queue Consumer (i.e. Lambda or EC2 instance)
  - Feature Logic (i.e. Privileges Grantor)

## Supported Apiary Hive Consumers

|ConsumerName|Connection to SQS| Subscribed Event Types | Description
|----|----|----|----
|PrivilegesGrantor|Lambda|CREATE_TABLE|Grants Public Role Privileges to a newly created table 
