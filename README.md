# Aggregate Concept Transformer (aggregate-concept-transformer)

[![Circle CI](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/aggregate-concept-transformer/tree/master) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/aggregate-concept-transformer/badge.svg)](https://coveralls.io/github/Financial-Times/aggregate-concept-transformer)

__A service for reading concepts from an Amazon s3 bucket and either serving them as a response or writing them to a kafka queue__

# Installation

For the first time:

`go get github.com/Financial-Times/aggregate-concept-transformer`

or update:

`go get -u github.com/Financial-Times/aggregate-concept-transformer`

# Running

```
$GOPATH/bin/aggregate-concept-transformer
--bucketName=com.ft.upp-concept-store
--topic=Concept
--kafkaAddress=localhost:9092
--awsRegion=eu-west-1
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

The bucketName, awsRegion, kafkaAddress and  topic arguments are mandatory. 

# Endpoints

Return concept from s3 with match uuid:

`http://localhost:8080/concept/{uuid} -X GET`

Post concept returned from s3 to kafka queue:

`http://localhost:8080/concept/{uuid} -X POST`

## Admin Endpoints
Health checks: `http://localhost:8080/__health`

Good to go: `http://localhost:8080/__gtg`