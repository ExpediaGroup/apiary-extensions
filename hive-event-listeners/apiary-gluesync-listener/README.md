# Apiary GlueSync event listener

## Overview
The GlueSync event listener is an optional Apiary component which, if enabled, will push metadata updates to an AWS Glue catalog.

## Installation
The listener can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaGroup/apiary-metastore-docker). 

## Configuration
The GlueSync listener can be configured by setting the following System Environment variables:

|Environment Variable|Required|Description|
|----|----|----|
GLUE_PREFIX|No|Prefix added to Glue databases to handle database name collisions when synchronizing multiple metastores to the Glue catalog.

## Table update SkipArchive
[AWS default](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html#Glue-UpdateTable-request-SkipArchive) is to archive the table on every update. This especially with Iceberg tables can lead to a lot of table version of which you can only have a certain limit. To counter this we override this property and set skipArchive=true so do *not* make an archive of the table when updating. 
If an archive is needed, this can be done per table by setting the Hive table property: 'apiary.gluesync.skipArchive=false'.


# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia, Inc.

