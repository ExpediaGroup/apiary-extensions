# Apiary ReadOnlyAuth pre-event listener

## Overview
The ReadOnlyAuth pre-event listener is an optional Apiary component which, if enabled, will handle authorization using a configurable database whitelist.

## Installation
The listener can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaInc/apiary-metastore-docker). 

## Configuration
The ReadOnly pre-event listener can be configured by setting the following System Environment variable:

|Environment Variable|Required|Description|
|----|----|----|
HIVE_DB_WHITELIST|Yes|Comma-separated database names to allow access.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia Inc.
