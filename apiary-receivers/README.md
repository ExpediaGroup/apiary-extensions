
# Overview

`apiary-receivers` provides common libraries for receiving messages from Apiary messaging infrastructure.  

## SQS

`apiary-receiver-sqs` is a library for polling SQS messages. 

To use, build `SqsMessageReader` with the required configuration:

| Property  | Required | Default | Description
|:---:|:---:|:---:|---|
|`queueUrl`|yes|-|SQS queue url|
|`waitTimeSeconds`|no|10|The default time to wait for messages while polling|
|`maxMessages`|no|10|The maximum number of messages to receive per read, acceptable values: 1 to 10|
|`messageDeserializer`|no|`DefaultSqsMessageDeserializer`|SQS message deserializer|
|`consumer`|no|Standard AmazonSQS client|AmazonSQS client|

# Contact

## Mailing List
If you would like to ask any questions about or discuss Apiary please join our mailing list at 

  [https://groups.google.com/forum/#!forum/apiary-user](https://groups.google.com/forum/#!forum/apiary-user)

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2019 Expedia, Inc.
