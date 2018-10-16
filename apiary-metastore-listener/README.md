# Apiary SNS event listener

##  Overview
The SNS event listener is an optional Apiary component which, if found on the Hive metastore classpath, will push 
Hive metastore events as JSON onto an Amazon Web Services SNS topic.

## Installation
The listener can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaInc/apiary-metastore-docker). 

## Configuration
The SNS listener can be configured by setting the following System Environment variables:

|Environment Variable|Required|Description|
|----|----|----|
SNS_ARN|Yes|The SNS topic ARN to which messages will be sent.

## JSON Messages
The following table describes all the fields that may be present in the JSON message that is sent to the SNS 
topic:

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version).
|`eventType`|String|Always|One of: CREATE_TABLE, DROP_TABLE, ALTER_TABLE, ADD_PARTITION, DROP_PARTITION, ALTER_PARTITION| 
|`dbName`|String|Always|The Hive database name|
|`tableName`|String|Always|The Hive table name|
|`oldTableName`|String|Only when `eventType` is ALTER_TABLE|The old Hive table name|
|`partition`|String|Only when the `eventType` is one of ADD_PARTITION, DROP_PARTITION, ALTER_PARTITION|The Hive partition values|
|`oldPartition`|String|Only when the `eventType` is ALTER_PARTITION|The old Hive partition values|

### Example Messages

#### Create table
The following shows an example JSON message representing a "CREATE_TABLE" event:

	{
      "protocolVersion": "1.0",
      "eventType": "CREATE_TABLE",
      "dbName": "some_db",
      "tableName": "some_table",
      "sourceMetastoreUris": "thrift://remote_host:9883"
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
      "partitionKeyValues": {"load_date": "2013-03-24","variant_code": "EN"}
    }
    
#### Alter Table
The following shows an example JSON message representing an "ALTER_TABLE" event:

    {
      "protocolVersion": "1.0",
      "eventType": "ALTER_TABLE",
      "dbName": "some_db",
      "tableName": "new_some_table",
      "sourceMetastoreUris": "thrift://remote_host:9883",
      "oldTableName": "some_table"
    }

#### Drop Table
The following shows an example JSON message representing a "DROP_TABLE" event:

    {
      "protocolVersion": "1.0",
      "eventType": "DROP_TABLE",
      "dbName": "some_db",
      "tableName": "some_table"
      "sourceMetastoreUris": "thrift://remote_host:9883"
    }
    
#### Add Partition
The following shows an example JSON message representing an "ADD_PARTITION" event:

    {
      "protocolVersion": "1.0",
      "eventType": "ADD_PARTITION",
      "dbName": "some_db",
      "tableName": "some_table",
      "sourceMetastoreUris": "thrift://remote_host:9883",
      "partitionKeys": ["col_1", "col_2", "col_3"],
      "partitionValues": ["val_1", "val_2", "val_3"],
    }

#### Drop Partition
The following shows an example JSON message representing a "DROP_PARTITION" event:

    {
      "protocolVersion": "1.0"
      "eventType": "DROP_PARTITION",
      "dbName": "some_db",
      "tableName": "some_table",
      "sourceMetastoreUris": "thrift://remote_host:9883",
      "partitionKeys": ["col_1", "col_2", "col_3"],
      "partitionValues": ["val_1", "val_2", "val_3"]
    }

#### Alter Partition
The following shows an example JSON message representing an "ALTER_PARTITION" event:

    {
      "protocolVersion": "1.0",
      "eventType": "ALTER_PARTITION",
      "dbName": "some_db",
      "tableName": "some_table",
      "sourceMetastoreUris": "thrift://remote_host:9883",
      "partitionKeys": ["col_1","col_2"],
      "partitionValues": ["val_1", "val_2", "val_3"],
      "oldPartitionValues": ["val_4", "val_5", "val_6"]
    }

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018 Expedia Inc.
