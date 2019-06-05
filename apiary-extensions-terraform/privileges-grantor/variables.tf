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
  description = "Bucket where the Lambda zip can be found, for example 'bucket_name' (Note with s3://). Used together with `pg_jars_s3_key`."
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
