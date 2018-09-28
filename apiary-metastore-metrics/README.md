# Apiary Metastore metrics reporter

## Overview
The metrics reporter is an optional Apiary component which, if enabled, will push metastore metrics to cloudwatch.

## Installation
The metrics reporter can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaInc/apiary-metastore-docker). 

## Configuration
The metrics reporter can be configured by setting the following System Environment variables:

|Environment Variable|Required|Description|
|----|----|----|
CLOUDWATCH_NAMESPACE|Yes|Custom namespace for metastore cloudwatch metrics.
ECS_TASK_ID|Yes|ecsTaskId dimension added to cloudwatch metrics to identify metastore container.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018 Expedia Inc.

