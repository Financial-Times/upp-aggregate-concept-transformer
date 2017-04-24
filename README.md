# Aggregate Concept Transformer (aggregate-concept-transformer)

[![Circle CI](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/aggregate-concept-transformer/badge.svg)](https://coveralls.io/github/Financial-Times/aggregate-concept-transformer)

__A service which gets notified of updates to concepts in an Amazon s3 bucket via sqs. It then returns all uuids with concordance to said concept, requests each in turn from s3, build concorded json model and writes updated concept to both Neo4j and Elastic Search__

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

The bucketName and queueUrl arguments are mandatory. 

# Endpoints

Return concorded concept from s3 with match uuid:

`http://localhost:8080/concept/{uuid} -X GET`

## Admin Endpoints
Health checks: `http://localhost:8080/__health`

Good to go: `http://localhost:8080/__gtg`

Ping: `http://localhost:8080/__ping`

Build info: `http://localhost:8080/__build-info`