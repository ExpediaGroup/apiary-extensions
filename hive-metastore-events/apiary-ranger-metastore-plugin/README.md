# Apiary RangerAuth pre event listener

## Overview
The RangerAuth pre event listener is an optional Apiary component which, if enabled, will handle authorization and auditing using Ranger.

## Installation
The listener can be activated by placing its jar file on the Hive metastore classpath and configuring Hive accordingly. For Apiary 
this is done in [apiary-metastore-docker](https://github.com/ExpediaGroup/apiary-metastore-docker). 

## Configuration
The RangerAuth pre event listener can be configured by copying the [ranger-hive-security.xml](https://github.com/apache/ranger/blob/master/hive-agent/conf/ranger-hive-security.xml) and [ranger-hive-audit.xml](https://github.com/apache/ranger/blob/master/hive-agent/conf/ranger-hive-audit.xml) files to the Hive config directory.

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2018-2019 Expedia, Inc.

