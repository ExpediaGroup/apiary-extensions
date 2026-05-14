# Apiary GlueSync event listener

## Overview
The GlueSync event listener is an optional Apiary component which, if enabled, will push metadata updates to an AWS Glue catalog.

In addition to the GlueSync event listener, this component contains a CLI that uses the same code for on-demand syncing in case there are operational failures and you need to provide a quick fix. That is documented in [README_CLI.md](README_CLI.md). 

## Installation
The listener can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaGroup/apiary-metastore-docker). 

## Configuration
The GlueSync listener can be configured by setting the following System Environment variables:

|Environment Variable|Required|Description|
|----|----|----|
GLUE_PREFIX|No|Prefix added to Glue databases to handle database name collisions when synchronizing multiple metastores to the Glue catalog.
ENABLE_HIVE_TO_GLUE_RENAME_OPERATION|No|Set to true in case you would like to enable Hive table renames when syncing into Glue. Default value is false.
GLUE_SKIP_ARCHIVE|No|Default value applied to `SkipArchive` on AWS Glue `UpdateTable` requests when a table does not set the `apiary.gluesync.skipArchive` property. Accepts `true` or `false` (case-insensitive). When unset, the built-in default (`true`) is used. Any other value causes the listener to fail on startup.

## Table update SkipArchive
[AWS default](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html#Glue-UpdateTable-request-SkipArchive) is to archive the table on every update. With Iceberg tables this can lead to a lot of table versions. In Glue you can only have a certain limit of the number of versions and you'll get exceptions when trying to update a table once you hit that limit. Manual version removal through AWS api is then needed. To counter this the listener defaults to `skipArchive=true`, so it does *not* make an archive of the table when updating.

The effective value is resolved using the following precedence (highest first):
1. The Hive table property `apiary.gluesync.skipArchive` (`true` or `false`), when set.
2. The environment variable `GLUE_SKIP_ARCHIVE` (`true` or `false`), when set.
3. The built-in default, `true`.

This setting only affects `ALTER TABLE` events. AWS Glue's `UpdatePartition` and `BatchUpdatePartition` APIs do not expose a `SkipArchive` field, so partition updates are not impacted.


# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia, Inc.
