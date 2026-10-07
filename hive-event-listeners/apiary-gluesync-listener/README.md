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
GLUE_SEND_VERSION_ID|No|When `true`, the listener sends the current Glue `VersionId` on `UpdateTable` requests for non-Iceberg (Hive) tables, so the emitted Glue/CloudTrail event carries it (see [Table update VersionId](#table-update-versionid)). Accepts `true` or `false` (case-insensitive). Default is `false`. Any other value causes the listener to fail on startup.

## Table update VersionId
When `GLUE_SEND_VERSION_ID=true`, the listener reads the current Glue `VersionId` (via `GetTable`) and passes it on `UpdateTable` for **non-Iceberg (Hive)** tables. This makes AWS log `requestParameters.versionId` on the emitted CloudTrail `UpdateTable` event — the value is the version the table was at immediately *before* the update, which lets consumers of the Glue/CloudTrail event stream identify and fetch the pre-update table state (via `GetTableVersion`) for `ALTER_TABLE` diffs. Iceberg tables are skipped because they can be identified downstream by `metadata_location`.

Two consequences of enabling it:
- AWS Glue treats `VersionId` as an optimistic-concurrency (compare-and-swap) token, so a concurrent writer causes the `UpdateTable` to fail with `ConcurrentModificationException`. The listener re-reads the current version and retries (bounded); if the retries are exhausted it falls back to an unconditional update (no `VersionId`), so the sync still succeeds — that one event just will not carry a `VersionId`. Likewise, if the current `VersionId` cannot be read at all (any Glue failure other than the table not existing), the listener updates unconditionally rather than failing the sync.
- For the carried `VersionId` to resolve to fetchable content, the prior version must be archived, so when this flag is on, Hive tables are forced to `skipArchive=false` (see below). An explicit per-table `apiary.gluesync.skipArchive` property still wins.

## Table update SkipArchive
[AWS default](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html#Glue-UpdateTable-request-SkipArchive) is to archive the table on every update. With Iceberg tables this can lead to a lot of table versions. In Glue you can only have a certain limit of the number of versions and you'll get exceptions when trying to update a table once you hit that limit. Manual version removal through AWS api is then needed. To counter this the listener defaults to `skipArchive=true`, so it does *not* make an archive of the table when updating.

The effective value is resolved using the following precedence (highest first):
1. The Hive table property `apiary.gluesync.skipArchive` (`true` or `false`), when set.
2. `false` for non-Iceberg (Hive) tables when `GLUE_SEND_VERSION_ID=true` (so the prior version is retained for the emitted `VersionId` to resolve — see [Table update VersionId](#table-update-versionid)).
3. The environment variable `GLUE_SKIP_ARCHIVE` (`true` or `false`), when set.
4. The built-in default, `true`.

This setting only affects `ALTER TABLE` events. AWS Glue's `UpdatePartition` and `BatchUpdatePartition` APIs do not expose a `SkipArchive` field, so partition updates are not impacted.


# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia, Inc.
