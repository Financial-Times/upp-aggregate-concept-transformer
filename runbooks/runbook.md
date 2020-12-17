# UPP - Aggregate Concept Transformer

A service that aggregates the information from all concept data providers and forwards it to the writer services for persistence in the concept stores.

## Code

aggregate-concept-transformer

## Primary URL

<https://upp-prod-delivery-glb.upp.ft.com/__aggregate-concept-transformer/>

## Service Tier

Bronze

## Lifecycle Stage

Production

## Delivered By

content

## Supported By

content

## Known About By

- dimitar.terziev
- elitsa.pavlova
- ivan.nikolov
- kalin.arsov
- miroslav.gatsanoga
- marina.chompalova

## Host Platform

AWS

## Architecture

The service receives notification of concept updates via Amazon SQS queue, aggregates several sources of concept information from Amazon S3 and via thr concordance data, ties them all together and posts the overall UPP view of the concept to both Neo4j and Elasticsearch, purges the Varnish cache, writes a notification in the concept events Amazon SQS queue and finally puts the updated IDs onto an Amazon Kinesis stream to be processed by FT.com.

## Contains Personal Data

No

## Contains Sensitive Data

No

## Dependencies

- concepts-rw-neo4j
- concordances-rw-neo4j
- up-crwes
- upp-concept-events-queue
- varnish-purger

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

The service does not store data, so it does not require any data recovery steps.

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

If the new release does not change the way messages are consumed, transformed or sent it is safe to deploy it without cluster failover.

## Key Management Process Type

Manual

## Key Management Details

To access the service clients need to provide basic auth credentials.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

Service in UPP K8S delivery clusters:

- Pub-Prod-EU health: <https://upp-prod-delivery-eu.upp.ft.com/__health/__pods-health?service-name=aggregate-concept-transformer>
- Pub-Prod-US health: <https://upp-prod-delivery-us.upp.ft.com/__health/__pods-health?service-name=aggregate-concept-transformer>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.
