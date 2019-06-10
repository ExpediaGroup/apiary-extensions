/**
 * Copyright (C) 2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

variable "instance_name" {
  description = "Privilege Grantor instance name to identify resources in multi-instance deployments."
  type        = "string"
  default     = ""
}

variable "subnets" {
  description = "Subnets in which Lambda will have access to."
  type        = "list"
}

variable "security_groups" {
  description = "Security groups in which Lambda will have access to."
  type        = "list"
}

variable "pg_lambda_bucket" {
  description = "Bucket where the Lambda zip can be found, for example 'bucket_name'. Used together with `pg_jars_s3_key`."
  type        = "string"
}

variable "pg_jars_s3_key" {
  description = "S3 key where zip file is located."
  type        = "string"
}

variable "pg_lambda_version" {
  description = "Version of the Privilege Grantor Lambda."
  type        = "string"
}

variable "pg_metastore_uri" {
  description = "Thrift URI of the metastore to which Lambda will connect to."
  type        = "string"
}

variable "metastore_events_sns_topic" {
  description = "SNS Topic for Hive Metastore events."
  type        = "string"
}

variable "metastore_events_filter" {
  description = "List of metastore event types to be added to SNS filter. Supported format: `<<EOD \"CREATE_TABLE\",\"ALTER_TABLE\" EOD`"
  type        = "string"
  default     = "\"CREATE_TABLE\",\"ALTER_TABLE\""
}

# Tags
variable "tags" {
  description = "A map of tags to apply to resources."
  type        = "map"

  default = {
    Environment = ""
    Application = ""
    Team        = ""
  }
}

variable "memory" {
  description = "The amount of memory (in MiB) to be used by Lambda"
  type        = "string"
  default     = "512"
}

variable "lambda_timeout" {
  description = "The time after which the lambda execution stops."
  type        = "string"
  default     = "200"
}
