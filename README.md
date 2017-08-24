# Aggregate Concept Transformer (aggregate-concept-transformer)

[![Circle CI](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/aggregate-concept-transformer)](https://goreportcard.com/report/github.com/Financial-Times/aggregate-concept-transformer)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/aggregate-concept-transformer/badge.svg)](https://coveralls.io/github/Financial-Times/aggregate-concept-transformer)

__A service which gets notified via SQS of updates to source concepts in an Amazon S3 bucket. It then returns all UUIDs with concordance to said concept, requests each in turn from S3, builds the concorded JSON model and sends the updated concept JSON to both Neo4j and Elasticsearch. Finally it sends a notification of all updated concepts to a kinesis stream and removes the SNS message from the queue__

# Installation

For the first time:

`go get github.com/Financial-Times/aggregate-concept-transformer`

or update:

`go get -u github.com/Financial-Times/aggregate-concept-transformer`

# Running

```
Usage: aggregate-concept-service [OPTIONS]

Aggregating and concording concepts in UPP.

Options:
  --app-system-code="aggregate-concept-transformer"       System Code of the application ($APP_SYSTEM_CODE)
  --app-name="Aggregate Concept Transformer"              Application name ($APP_NAME)
  --port="8080"                                           Port to listen on ($APP_PORT)
  --bucketRegion="eu-west-1"                              AWS Region in which the S3 bucket is located ($BUCKET_REGION)
  --sqsRegion=""                                          AWS Region in which the SQS queue is located ($SQS_REGION)
  --bucketName=""                                         Bucket to read concepts from. ($BUCKET_NAME)
  --queueUrl=""                                           Url of AWS sqs queue to listen to ($QUEUE_URL)
  --messagesToProcess=10                                  Maximum number or messages to concurrently read off of queue and process ($MAX_MESSAGES)
  --visibilityTimeout=30                                  Duration(seconds) that messages will be ignored by subsequent requests after initial response ($VISIBILITY_TIMEOUT)
  --waitTime=20                                           Duration(seconds) to wait on queue for messages until returning. Will be shorter if messages arrive ($WAIT_TIME)
  --neo4jWriterAddress="http://localhost:8080/"           Address for the Neo4J Concept Writer ($NEO_WRITER_ADDRESS)
  --elasticsearchWriterAddress="http://localhost:8080/"   Address for the Elasticsearch Concept Writer ($ES_WRITER_ADDRESS)
  --dynamoDBTable="concordances"                          DynamoDB table to read concordances from ($DYNAMODB_TABLE)
  --dynamoDBTable="eu-west-1"                             AWS region the DynamoDB table is in ($DYNAMODB_REGION)
  --kinesisStreamName="upp-stream-name"                   Kinesis stream name to send notifications of updated uuids to ($KINESIS_STREAM_NAME)
  --kinesisRegion="eu-west-1"                             AWS region the Kinesis stream is in ($KINESIS_REGION)
  --logLevel="info"                                       App log level ($LOG_LEVEL)
  --requestLoggingOn="true"                               Whether to log http requests/responses ($REQUEST_LOGGING_ON)
```


The app assumes that you have correctly set up your AWS credentials by either using the `~/.aws/credentials` file:

```
[default]
aws_access_key_id = AKID1234567890
aws_ secret_access_key = MY-SECRET-KEY
```

or the default AWS environment variables otherwise requests will return 401 Unauthorised

```
AWS_ACCESS_KEY_ID=AKID1234567890
AWS_SECRET_ACCESS_KEY=MY-SECRET-KEY
```

## Build and deployment

Tests require a running local instance of DynamoDB.  The easiest way is with Docker:
`docker run -d -p 8000:8000 dwmkerr/dynamodb`

* Built by Docker Hub when merged to master: [coco/aggregate-concept-transformer](https://hub.docker.com/r/coco/aggregate-concept-transformer/)
* CI provided by CircleCI: [aggregate-concept-transformer](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer)
* Code test coverage provided by Coveralls: [aggregate-concept-transformer](https://coveralls.io/github/Financial-Times/aggregate-concept-transformer)

## Aggregation
This service aggregates a number of source concepts into a single canonical view.  At present, the logic is as follows:
- All concorded/secondary concepts are merged together without any ordering.  These will always be from TME at the moment.
- The primary concept is then merged, overwriting the fields from the secondary concepts.  This is a Smartlogic concept.
- Aliases are the exception - they are merged between all concepts and de-duplicated.

## Endpoints

See [swagger.yml](api/swagger.yml)

## Admin Endpoints

* Health checks: `http://localhost:8080/__health`
* Good to go: `http://localhost:8080/__gtg`
* Build info: `http://localhost:8080/__build-info`

## Documentation

* API Doc: [API Doc](https://docs.google.com/document/d/1FSJBuAq_cncxqr-qsuzQMRcrejiPHWc41cnrpiJ3Gsc/edit)
* Runbook: [Runbook](https://dewey.ft.com/aggregate-concept-transformer.html)
* Panic Guide: [API Doc](https://docs.google.com/document/d/1FSJBuAq_cncxqr-qsuzQMRcrejiPHWc41cnrpiJ3Gsc/edit)
