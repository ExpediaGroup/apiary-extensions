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

## Syncing a single partition

By default the CLI syncs every partition of each matched table. To sync just one partition instead, use either
`--partition-name` or `--partition-values` (mutually exclusive — pick one).

`--database-name-regex`/`--table-name-regex` must match **exactly one table** when either option is used (the CLI
errors out otherwise) — use anchored, literal regexes (e.g. `^mydb$`) rather than wildcards.

### `--partition-name` (recommended)

Takes the partition name exactly as printed by Hive's `SHOW PARTITIONS` — `key=value` pairs joined by `/`. The CLI
looks up the table's partition keys and reorders the values accordingly, so the key order in the string doesn't
need to match the table's declared order.

Given:

```
hive> show partitions stg_customer_segmentation.sync_runs;
OK
sync_id=2235348/sync_run_id=493558413
...
```

you can pass that partition name straight through:

```
AWS_REGION=us-east-1 THRIFT_CONNECTION_URI=thrift://my.hive.service:9083 \
  java -jar ./target/apiary-gluesync-listener-8.1.12-cli.jar \
  --database-name-regex '^stg_customer_segmentation$' \
  --table-name-regex '^sync_runs$' \
  --partition-name 'sync_id=2235348/sync_run_id=493558413' \
  -v
```

If any key in `--partition-name` doesn't match one of the table's partition keys, the CLI errors out rather than
guessing.

### `--partition-values`

Takes just the raw values, comma separated, in the table's partition-key order (no `key=value`, no `/`). For the
same partition as above (partition keys `sync_id`, `sync_run_id`):

```
--partition-values 2235348,493558413
```

This is more error-prone than `--partition-name` if you get the key order wrong, but is handy for scripting when
you already have the ordered values.

### Behavior

Either option only touches the one requested partition (create/update in Glue to match Hive, or delete from Glue if
it no longer exists in Hive and `--keep-glue-partitions` is not set) — no other partitions of the table are
affected.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia, Inc.
