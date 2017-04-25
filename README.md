# Aggregate Concept Transformer (aggregate-concept-transformer)

[![Circle CI](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master) [![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/aggregate-concept-transformer)](https://goreportcard.com/report/github.com/Financial-Times/aggregate-concept-transformer) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/aggregate-concept-transformer/badge.svg)](https://coveralls.io/github/Financial-Times/aggregate-concept-transformer)

__A service which gets notified via SQS of updates to source concepts in an Amazon s3 bucket. It then returns all uuids with concordance to said concept, requests each in turn from s3, builds the concorded json model and sends the updated concept json to both Neo4j and Elastic Search__

# Installation

For the first time:

`go get github.com/Financial-Times/aggregate-concept-transformer`

or update:

`go get -u github.com/Financial-Times/aggregate-concept-transformer`

# Running

```
$GOPATH/bin/aggregate-concept-transformer
--bucketName=com.ft.upp-concept-store     
--awsRegion=eu-west-1
--queueUrl=https://amazonSqs/sqsQueue
--messagesToProcess=10
--visibilityTimeout=10
--waitTime=10
--vulcanAddress=http://localhost:8080/
--port=8080
```


The bucketName, awsRegion and queueUrl arguments are mandatory. 
VulcanAddress is used when deployed to containers for routing purposes. 
MessagesToProcess is the maximum number or messages the app will concurrently read off of queue and process.
VisbilityTimeout is the length of time messages will wait on queue after processing failure before being retried.
WaitTime is length of time app will listen to queue for messages before returning.

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

* Built by Docker Hub when merged to master: [coco/aggregate-concept-transformer](https://hub.docker.com/r/coco/aggregate-concept-transformer/)
* CI provided by CircleCI: [aggregate-concept-transformer](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer)
* Code test coverage provided by Coveralls: [aggregate-concept-transformer](https://coveralls.io/github/Financial-Times/aggregate-concept-transformer)

## Endpoints

See swagger.yml

## Admin Endpoints

Health checks: `http://localhost:8080/__health`
Good to go: `http://localhost:8080/__gtg`
Ping: `http://localhost:8080/__ping`
Build info: `http://localhost:8080/__build-info`

## Documentation

* API Doc: [API Doc](https://docs.google.com/document/d/1FSJBuAq_cncxqr-qsuzQMRcrejiPHWc41cnrpiJ3Gsc/edit)
* Runbook: [Runbook](https://dewey.ft.com/aggregate-concept-transformer.html)
* Panic Guide: [API Doc](https://docs.google.com/document/d/1FSJBuAq_cncxqr-qsuzQMRcrejiPHWc41cnrpiJ3Gsc/edit)
