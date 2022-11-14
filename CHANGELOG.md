# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## 7.3.6 - 2022-11-14
### Fixed
- `apiary-gluesync-listener` when getting null `SortOrder` in Hive & Iceberg tables.

## 7.3.5 - 2022-11-11
### Fixed
- `apiary-gluesync-listener` when getting last access time in Hive & Iceberg tables.

## 7.3.4 - 2022-11-02
### Changed
- Add `aws-java-sdk-sts` to enable IRSA authentication in `apiary-gluesync-listener`.

## 7.3.3 - 2022-10-03
### Changed
- `aws-java-sdk` version to `1.12.276` (was `1.11.333`) so we can switch from KIAM to IRSA.

## 7.3.2 - 2022-05-31
### Changed
- ` hive-hooks` - Logging table name when no conversion can take place.

## 7.3.1 - 2022-05-27
### Added
- ` hive-hooks` - Fixing potential NullPointerException in `ApiaryMetastoreFilter` and `PathConverter`.

## 7.3.0 - 2022-05-24
### Added
- `hive-event-listeners/apiary-gluesync-listener` - Added implementation for Glue database sync.

## 7.2.5 - 2022-03-01
### Fixed
- `hive-event-listeners/apiary-gluesync-listener` added fix for NullPointerException on unpartitioned tables.
### Added
- `log4j` managed dependencies to overwrite transitive dependencies with log4j security issues being pulled in. These dependencies come from Hive and are provided but they break our tests.

## 7.2.4 - 2021-10-12
- Same as `7.2.2`. Released while configuring GitHub actions workflow.

## 7.2.3 - 2021-10-12
- Same as `7.2.2`. Released while configuring GitHub actions workflow.

## 7.2.2 - 2021-05-27
### Fixed
- `hive-hooks` component to convert Storage Descriptor Info 'path' parameter (instead of Storage Descriptor).

## 7.2.1 - 2021-05-25
### Changed
- `hive-hooks` component to convert Storage Descriptor 'path' parameter.
- `hive-hooks` component to convert table parameter 'avro.schema.url'.

### Removed
- Excluded `org.pentaho:pentaho-aggdesigner-algorithm` from `kafka-metastore-events`, `apiary-gluesync-listener`, `apiary-metastore-auth` and `apiary-ranger-metastore-plugin`.

## 7.2.0 - 2021-03-16
### Added
- Added `hive-hooks` component for generic path and table location modification.

## 7.1.0 - 2021-02-18
### Changed
- Relocated Kafka libraries in `kafka-metastore-listener` under `com.expediagroup.apiary.extensions.shaded.org.apache.kafka` to avoid clashes when moving other components to a newer version of Kafka.
- `eg-oss-parent` version updated to 2.3.2 (was 1.2.0).
- `jdk.tools` excluded as a transitive dependency everywhere to allow compilation on Java 11.
- A few transitive dependencies made explicit to allow compilation on Java 11.

## 7.0.0 - 2020-04-29
### Changed
- Renamed `hive-metastore-events` submodule to `hive-event-listeners`.

## 6.0.2 - 2020-04-24
### Changed
- Upgraded version of `hive.version` to `2.3.7` (was `2.3.3`). Allows apiary-extensions to be used on JDK>=9.

## 6.0.1 - 2020-04-02
### Fixed
- Bug in `privileges-grantor-lambda` where Apiary Events were not being deserialized properly.

## 6.0.0 - 2020-03-12
### Changed
- `client.id` is now required to configure Listener.
- `topic` Kafka configuration parameter is now called `topic.name`.
- No longer sending MetaStore uris with Kafka message.
- Kafka Receiver is now configured using a builder.

## 5.0.2 - 2020-02-04
### Changed
- `eg-oss-parent` version updated to 1.2.0 (was 1.1.0).
### Added
- `sns-metastore-event` SNS message attribute: `qualifiedTableName`.

## 5.0.1 - 2020-01-10
### Changed
- Upgraded Solr version to `7.7.1` (was `5.5.4`) to fix issue where Ranger plugin jar (version `2.0.0`) could not instantiate Solr client correctly.

## 5.0.0 - 2019-12-09
### Changed
- Moved `apiary-gluesync-listener`, `apiary-metastore-auth` and `apiary-ranger-metastore-plugin` (all previously top-level modules) to be submodules under a new top-level module named `hive-metastore-events`.
- Grouped `apiary-metastore-consumers`, `apiary-metastore-listener` and `apiary-receivers` (all previously submodules in `apiary-metastore-events`) in a new module named `sns-metastore-events`, itself a submodule of `apiary-metastore-events`.
- Updated `apiary-ranger-metastore-plugin` to use Ranger 2.0.0 (was 1.1.0).
- Changed parent to `com.expediagroup:eg-oss-parent:1.1.0` (was `com.hotels:hotels-oss-parent:4.0.1`).

## 4.2.0 - 2019-08-02
### Added
- `apiary-ranger-metastore-plugin` module has an `ApiaryRangerAuthAllAccessPolicyProvider` class that can be configured in `ranger-hive-security.xml` to allow for an audit-only Ranger pre-event listener. Intent is to configure it on the Apiary read-only HMS instances.

## 4.1.0 - 2019-06-27
### Added
- `privileges-grantor-lambda` supports granting access to tables that were renamed.
- `apiary-metastore-consumers` sub-module of `apiary-metastore-events`.
- `metastore-consumer-common` module that contains common metastore consumer classes.
- `privileges-grantor-core` module that grants "SELECT" privileges on a table.
- `privileges-grantor-lambda` module that triggers `privileges-grantor-core` logic from an AWS Lambda.
- `DbName` and `TableName` as additional message attributes for SNS.

## [4.0.0] - 2019-05-08
### Added
- `apiary-metastore-events` module and moved `apiary-metastore-listener` and `apiary-receivers` into this.

### Changed
- `apiary-metastore-listener` and `apiary-receivers` package paths renamed from `com.expediagroup.apiary.extensions` to `com.expediagroup.apiary.extensions.events` (if you refer to these classes by fully qualified name downstream you will need to update these references before upgrading to this version).

## [3.1.0] - 2019-05-03
### Fixed
- `apiary-metastore-listener` now serializes metastore table properties as a Map instead of a String.

## [3.0.0] - 2019-04-12
### Changed
- Maven group ID is now `com.expediagroup.apiary` (was `com.expedia.apiary`).
- All Java classes moved to `com.expediagroup.apiary` (was `com.expedia.apiary`).
- `hotels-oss-parent` version updated to 4.0.1 (was 2.3.5).

## [2.0.0] - 2019-04-08
### Changed
- `MessageReader.read()` now returns `MessageEvent`, wrapping `ListenerEvent` and `MessageProperty`.

### Added
- `SqsMessageReader.read()` no longer deletes messages. Instead, `SqsMessageReader.delete()` has been added to give more control over inflight messages.

### Removed
- `aws-java-sdk` dependency from `apiary-receiver-sqs`.

## [1.4.0] - 2019-03-27
### Added
- `apiary-receivers` parent module and `apiary-receiver-sqs` sub-module, a library to poll Apiary SQS messaging infrastructure for Hive events.

## [1.3.2] - 2019-03-22
### Fixed
- Metastore events not published to SNS due to the Message Attribute type being undefined.

### Added
- Filtering of the SNS messages produced in ApiarySNSListener using the `eventType` Message Attribute.  

## [1.3.0] - 2019-03-14 [[YANKED]]
### Added
- Filtering of the SNS messages produced in ApiarySNSListener using the `eventType` Message Attribute.

## [1.2.0] - 2019-03-13
### Added
- Old locations for Table and Partitions to ApiarySNSListener events.
- Table parameters with a configurable regular expression to control which table parameters will be added to the ApiarySNSListener message. Default will be none.

## [1.1.0] - 2019-02-15
### Added
- Apiary ReadOnly Auth Event Listener.

### Changed
- Upgraded `hotels-oss-parent` version to 2.3.5 (was 2.3.3).

### Removed
- Transitive dependency on `org.apache.hbase` `hbase-client`. See [#18](https://github.com/ExpediaGroup/apiary-extensions/issues/18).

## [1.0.0] - 2018-10-31
### Added
- Partition Keys with Data types added to the JSON Events from ApiarySNSListener.

### Changed
- `partition` is renamed to `partitionValues` and `oldPartition` to `oldPartitionValues` in the JSON Events from ApiarySNSListener.

### Fixed
- JSON Events from ApiarySNSListener now marshals list as a JSONArray.

## [0.2.0] - 2018-10-03
### Added
- `apiary-gluesync-listener` sub-module.
- `apiary-metastore-metrics` sub-module.
- `apiary-ranger-metastore-plugin` sub-module.

## [0.1.0] - 2018-09-11
### Changed
- Apiary SNS listener now configured using standard AWS configuration mechanisms. See [#4](https://github.com/ExpediaGroup/apiary-extensions/issues/4).

### Added
- A `protocolVersion` field to the SNS messages generated by `ApiarySnsListener`. See [#2](https://github.com/ExpediaGroup/apiary-extensions/issues/2).
- Support for `INSERT` metastore events. See [#6](https://github.com/ExpediaGroup/apiary-extensions/issues/6).

## [0.0.1] - 2018-08-23
### Added
- Initial version to test that releases to Maven Central work.
