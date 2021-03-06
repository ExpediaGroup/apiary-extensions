# Apiary SNS event listener

##  Overview
The SNS event listener is an optional Apiary component which, if found on the Hive metastore classpath, will push 
Hive metastore events as JSON onto an Amazon Web Services SNS topic.

![Apiary SNS event listener Architecture.](images/Apiary_SNS_event_listener.png  "Apiary SNS event listener.")

## Installation
The listener can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaGroup/apiary-metastore-docker). 

## Configuration
The SNS listener can be configured by setting the following System Environment variables:

|Environment Variable|Required|Description|
|----|----|----|
SNS_ARN|Yes|The SNS topic ARN to which messages will be sent.
TABLE_PARAM_FILTER|No|A regular expression for selecting necessary table parameters. If the value isn't set, then no table parameters are selected.

## JSON Messages
The following table describes all the fields that may be present in the JSON message that is sent to the SNS 
topic:

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version).
|`eventType`|String|Always|One of: CREATE_TABLE, DROP_TABLE, ALTER_TABLE, ADD_PARTITION, DROP_PARTITION, ALTER_PARTITION| 
|`dbName`|String|Always|The Hive database name|
|`tableName`|String|Always|The Hive table name|
|`tableLocation`|String|In all events except INSERT|The Hive table location|
|`tableParameters`|String|In all events except INSERT when TABLE_PARAM_FILTER is set|The Hive table parameters|
|`oldTableName`|String|Only when `eventType` is ALTER_TABLE|The old Hive table name|
|`partition`|String|Only when the `eventType` is one of ADD_PARTITION, DROP_PARTITION, ALTER_PARTITION|The Hive partition values|
|`partitionLocation`|String|Only when the `eventType` is one of ADD_PARTITION, DROP_PARTITION, ALTER_PARTITION|The Hive partition location|
|`oldPartition`|String|Only when the `eventType` is ALTER_PARTITION|The old Hive partition values|
|`oldPartitionLocation`|String|Only when the `eventType` is ALTER_PARTITION|The old Hive partition location|

### Example Messages

#### Create table
The following shows an example JSON message representing a "CREATE_TABLE" event when the environment variable TABLE_PARAM_FILTER is set to `my_var.*`:

	{
       "protocolVersion": "1.0",
       "eventType": "CREATE_TABLE",
       "dbName": "some_db",
       "tableName": "some_table",
       "tableLocation": "s3://table_location",
       "tableParameters": {
           "my_var_one": "val_one",
           "my_var_two": "val_two"
        }
	}
	
#### Insert
The following shows an example JSON message representing an "INSERT" event:

    {
      "protocolVersion": "1.0",
      "eventType": "INSERT",
      "dbName": "some_db",
      "tableName": "some_table",
      "files": ["file:/a/b.txt","file:/a/c.txt"],
      "fileChecksums": ["123","456"],
      "partitionKeyValues": {
          "load_date": "2013-03-24",
          "variant_code": "EN"
       }
    }
    
#### Alter Table
The following shows an example JSON message representing an "ALTER_TABLE" event when the environment variable TABLE_PARAM_FILTER is set to `my_var.*`:

    {
      "protocolVersion": "1.0",
      "eventType": "ALTER_TABLE",
      "dbName": "some_db",
      "tableName": "new_some_table",
      "tableLocation": "s3://table_location",
      "tableParameters": {
           "my_var_one": "val_one",
           "my_var_two": "val_two"
       },
      "oldTableName": "some_table",
      "oldTableLocation": "s3://old_table_location"
    }

#### Drop Table
The following shows an example JSON message representing a "DROP_TABLE" event:

    {
      "protocolVersion": "1.0",
      "eventType": "DROP_TABLE",
      "dbName": "some_db",
      "tableName": "some_table",
      "tableLocation": "s3://table_location"
    }
    
#### Add Partition
The following shows an example JSON message representing an "ADD_PARTITION" event:

    {
      "protocolVersion": "1.0",
      "eventType": "ADD_PARTITION",
      "dbName": "some_db",
      "tableName": "some_table",
      "tableLocation": "s3://table_location",
      "partitionKeys": {
          "column_1": "string",
          "column_2": "int",
          "column_3": "string"
       },
      "partitionValues": ["value_1","1000","value_2"],
      "partitionLocation": "s3://table_location/partition"
    }

#### Drop Partition
The following shows an example JSON message representing a "DROP_PARTITION" event:

    {
      "protocolVersion": "1.0"
      "eventType": "DROP_PARTITION",
      "dbName": "some_db",
      "tableName": "some_table",
      "tableLocation": "s3://table_location",
      "partitionKeys": {
          "column_1": "string",
          "column_2": "int",
          "column_3": "string"
       },
      "partitionValues": ["value_1","1000","value_2"],
      "partitionLocation": "s3://table_location/partition"
    }

#### Alter Partition
The following shows an example JSON message representing an "ALTER_PARTITION" event:

    {
      "protocolVersion": "1.0",
      "eventType": "ALTER_PARTITION",
      "dbName": "some_db",
      "tableName": "some_table",
      "tableLocation": "s3://table_location",
      "partitionKeys": {
          "column_1": "string",
          "column_2": "int",
          "column_3": "string"
       },
      "partitionValues": ["value_3","2000","value_4"],
      "partitionLocation": "s3://table_location/partition"
      "oldPartitionValues": ["value_1","1000","value_2"],
      "oldPartitionLocation": "s3://table_location/old_partition"
    }

## Message Attributes
The [Message attributes](https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html) enable filtering of the SNS events. This can be done by applying a filter policy in a subscription receiver.
The following messages attributes are supported:

|Field Name|Type| Description
|----|----|----
|`eventType`|String|One of: CREATE_TABLE, DROP_TABLE, ALTER_TABLE, ADD_PARTITION, DROP_PARTITION, ALTER_PARTITION
|`dbName`|String|Database name of the Hive table from which the event is emitted.
|`tableName`|String|Name of the Hive table from which the event is emitted.
|`qualifiedTableName`|String|Combined version of dbName and tableName: `my_db.my_table`.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
