# Overview

Terraform module for setting up infrastructure for [Apiary Privilege Grantor](https://github.com/ExpediaGroup/apiary-extensions/tree/master/apiary-metastore-events/apiary-metastore-consumers/privileges-grantor).

For more information please refer to the main [Apiary](https://github.com/ExpediaInc/apiary) project page.

## Variables

| Name | Description | Type | Default | Required |
|------|-------------|:----:|:-----:|:-----:|
| instance\_name | Privilege Grantor instance name to identify resources in multi-instance deployments. | string | `""` | no |
| memory | The amount of memory (in MiB) to be used by Lambda | string | `"512"` | no |
| pg\_jars\_s3\_key | S3 key where zip file is located. | string | n/a | yes |
| pg\_lambda\_bucket | Bucket where the Lambda zip can be found, for example 'bucket_name' (Note with s3://). Used together with `pg_jars_s3_key`. | string | n/a | yes |
| pg\_lambda\_version | Version of the Privilege Grantor Lambda. | string | n/a | yes |
| pg\_metastore\_uri | Thrift URI of the metastore to which Lambda will connect to. | string | n/a | yes |
| security\_groups | Security groups in which Lambda will have access to. | list | n/a | yes |
| subnets | Subnets in which Lambda will have access to. | list | n/a | yes |
| tags | A map of tags to apply to resources. | map | `<map>` | no |

## Usage

Example module invocation:
```
module "apiary-privilege-grantor" {
  source = "git@github.com:HotelsDotCom/apiary-extensions-terraform.git/privileges-grantor"
  pg_lambda_bucket  = "pg-s3-bucket"
  pg_jars_s3_key    = "pg-s3-key"
  pg_lambda_version = "4.1.0"
  pg_metastore_uri  = "thrift://ip-address:9083"
  subnets           = ["subnet-1", "subnet-2"]
  security_groups   = ["security-group-1", "security-group-2"]
  tags = {
    Name = "Apiary-Privilege-Grantor"
    Team = "Operations"
  }
}

```

# Contact

## Mailing List
If you would like to ask any questions about or discuss Apiary please join our mailing list at

  [https://groups.google.com/forum/#!forum/apiary-user](https://groups.google.com/forum/#!forum/apiary-user)

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia Inc.
