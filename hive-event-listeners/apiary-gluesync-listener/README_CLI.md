# Apiary GlueSync CLI

## Overview
The GlueSync CLI is a CLI that uses the same code as the hive event listener (with some additions) for on-demand syncing of a Hive metastore table to an AWS Glue database.

One major addition for the GlueSync CLI is that it will always sync all partitions (if needed) for a given Hive table as opposed to the event driven sync which expects to receive individual events for each partition.

## Installation
The CLI gets packaged into a single fat jar with all dependencies so that you can run it simply by java -jar <PATHTOJAR>. The jar name is apiary-gluesync-listener-<version>-cli.jar.

# Usage
You need to provide two environment vars, AWS_REGION and THRIFT_CONNECTION_URI. Be careful not to point the THRIFT_URI to a waggledance endpoint or you may sync federated tables (unless you actually want that).

Example:

```
AWS_REGION=us-east-1 THRIFT_CONNECTION_URI=thrift://my.hive.service:9083 java -jar ./target/apiary-gluesync-listener-8.1.12-cli.jar --database-name-regex 'lz_.*' --table-name-regex 'bookings.*' -v
```

For more usage details, pass the "-h" flag.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia, Inc.
