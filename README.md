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
--awsAccessKey=xxx
--awsSecretKey=xxx
--bucketName=com.ft.upp-concept-store
--topic=Concepts
--kafkaAddress=localhost:9092
--awsRegion=eu-west-1
--port=8080
```

The awsAccessKey, awsSecretKey, bucketName, kafkaAddress and awsRegion arguments are mandatory and represent authentication credentials for S3 . 

The resources argument specifies a comma separated list of archives and files within that archive to be downloaded from Factset FTP server. Because every file is inside an archive, the service will first download the archive, unzip the files you specify, zip a collection of daily/weekly files and upload the resulting zips to s3. A resource has the format archive_path:file1.txt;file2.txt, example: /datafeeds/edm/edm_bbg_ids/edm_bbg_ids:edm_bbg_ids.txt, where  /datafeeds/edm/edm_bbg_ids/ is the path of the archive, edm_bbg_ids is the prefix of the zip without versions and edm_bbg_ids.txt is the file to be extracted from this archive. On the Factset FTP server the archive name will contain also the data version, but it is enough for this service to provide the archive name without the version and it will download the latest one.

After downloading the zip files from Factset FTP server, the service will write them to the specified Amazon S3 bucket. The zip files written to S3 will be inside of a folder named by the current date. Depending upon the day there may be both a weekly.zip and daily.zip or just a daily.zip

# Endpoints

Force-import (initiate importing manually of all most recent files):

`http://localhost:8080/force-import -XPOST`

Force-import-weekly (initiate importing manually of all most recent weekly files):

`http://localhost:8080/force-import-weekly -XPOST`

## Admin Endpoints
Health checks: `http://localhost:8080/__health`

Good to go: `http://localhost:8080/__gtg`