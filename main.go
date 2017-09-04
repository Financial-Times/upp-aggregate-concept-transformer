package main

import (
	"net/http"
	"os"

	"net"
	"time"

	"github.com/Financial-Times/aggregate-concept-transformer/concept"
	"github.com/Financial-Times/aggregate-concept-transformer/dynamodb"
	"github.com/Financial-Times/aggregate-concept-transformer/kinesis"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	_ "github.com/joho/godotenv/autoload"
	log "github.com/sirupsen/logrus"
)

const appDescription = "Service to aggregate concepts from different sources and produce a canonical view."

func main() {
	app := cli.App("aggregate-concept-service", "Aggregating and concording concepts in UPP.")

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "aggregate-concept-transformer",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})
	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "Aggregate Concept Transformer",
		Desc:   "Application name",
		EnvVar: "APP_NAME",
	})
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})
	bucketRegion := app.String(cli.StringOpt{
		Name:   "bucketRegion",
		Desc:   "AWS Region in which the S3 bucket is located",
		Value:  "eu-west-1",
		EnvVar: "BUCKET_REGION",
	})
	sqsRegion := app.String(cli.StringOpt{
		Name:   "sqsRegion",
		Desc:   "AWS Region in which the SQS queue is located",
		EnvVar: "SQS_REGION",
	})
	bucketName := app.String(cli.StringOpt{
		Name:   "bucketName",
		Desc:   "Bucket to read concepts from.",
		EnvVar: "BUCKET_NAME",
	})
	queueURL := app.String(cli.StringOpt{
		Name:   "queueUrl",
		Desc:   "Url of AWS sqs queue to listen to",
		EnvVar: "QUEUE_URL",
	})
	messagesToProcess := app.Int(cli.IntOpt{
		Name:   "messagesToProcess",
		Value:  10,
		Desc:   "Maximum number or messages to concurrently read off of queue and process",
		EnvVar: "MAX_MESSAGES",
	})
	visibilityTimeout := app.Int(cli.IntOpt{
		Name:   "visibilityTimeout",
		Value:  30,
		Desc:   "Duration(seconds) that messages will be ignored by subsequent requests after initial response",
		EnvVar: "VISIBILITY_TIMEOUT",
	})
	waitTime := app.Int(cli.IntOpt{
		Name:   "waitTime",
		Value:  20,
		Desc:   "Duration(seconds) to wait on queue for messages until returning. Will be shorter if messages arrive",
		EnvVar: "WAIT_TIME",
	})
	neoWriterAddress := app.String(cli.StringOpt{
		Name:   "neo4jWriterAddress",
		Value:  "http://localhost:8080/",
		Desc:   "Address for the Neo4J Concept Writer",
		EnvVar: "NEO_WRITER_ADDRESS",
	})
	elasticsearchWriterAddress := app.String(cli.StringOpt{
		Name:   "elasticsearchWriterAddress",
		Value:  "http://localhost:8080/",
		Desc:   "Address for the Elasticsearch Concept Writer",
		EnvVar: "ES_WRITER_ADDRESS",
	})
	dynamoDBTable := app.String(cli.StringOpt{
		Name:   "dynamoDBTable",
		Value:  "concordances",
		Desc:   "DynamoDB table to read concordances from",
		EnvVar: "DYNAMODB_TABLE",
	})
	dynamoDBRegion := app.String(cli.StringOpt{
		Name:   "dynamoDBTable",
		Value:  "eu-west-1",
		Desc:   "AWS region the DynamoDB table is in",
		EnvVar: "DYNAMODB_REGION",
	})
	kinesisStreamName := app.String(cli.StringOpt{
		Name:   "kinesisStreamName",
		Desc:   "DynamoDB table to read concordances from",
		EnvVar: "KINESIS_STREAM_NAME",
	})
	kinesisRegion := app.String(cli.StringOpt{
		Name:   "kinesisRegion",
		Value:  "eu-west-1",
		Desc:   "AWS region the kinesis stream is located",
		EnvVar: "KINESIS_REGION",
	})
	requestLoggingOn := app.Bool(cli.BoolOpt{
		Name:   "requestLoggingOn",
		Value:  true,
		Desc:   "Whether to log http requests or not",
		EnvVar: "REQUEST_LOGGING_ON",
	})
	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "info",
		Desc:   "App log level",
		EnvVar: "LOG_LEVEL",
	})

	app.Action = func() {

		log.SetFormatter(&log.JSONFormatter{})
		lvl, err := log.ParseLevel(*logLevel)
		if err != nil {
			log.WithField("LOG_LEVEL", *logLevel).Warn("Cannot parse log level, setting it to INFO.")
			lvl = log.InfoLevel
		}
		log.SetLevel(lvl)

		log.WithFields(log.Fields{
			"DYNAMODB_TABLE":      *dynamoDBTable,
			"ES_WRITER_ADDRESS":   *elasticsearchWriterAddress,
			"NEO_WRITER_ADDRESS":  *neoWriterAddress,
			"BUCKET_REGION":       *bucketRegion,
			"BUCKET_NAME":         *bucketName,
			"SQS_REGION":          *sqsRegion,
			"QUEUE_URL":           *queueURL,
			"LOG_LEVEL":           *logLevel,
			"KINESIS_STREAM_NAME": *kinesisStreamName,
		}).Info("Starting app with arguments")

		if *bucketName == "" {
			log.Fatal("S3 bucket name not set")
			return
		}
		if *queueURL == "" {
			log.Fatal("SQS queue url not set")
			return
		}

		if *bucketRegion == "" {
			log.Fatal("AWS bucket region not set")
		}

		if *sqsRegion == "" {
			log.Fatal("AWS SQS region not set")
		}

		if *kinesisStreamName == "" {
			log.Fatal("Kinesis stream name not set")
		}

		s3Client, err := s3.NewClient(*bucketName, *bucketRegion)
		if err != nil {
			log.WithError(err).Fatal("Error creating S3 client")
		}

		sqsClient, err := sqs.NewClient(*sqsRegion, *queueURL, *messagesToProcess, *visibilityTimeout, *waitTime)
		if err != nil {
			log.WithError(err).Fatal("Error creating SQS client")
		}

		dynamoClient, err := dynamodb.NewClient(*dynamoDBRegion, *dynamoDBTable)
		if err != nil {
			log.WithError(err).Fatal("Error creating DynamoDB client")
		}

		kinesisClient, err := kinesis.NewClient(*kinesisStreamName, *kinesisRegion)
		if err != nil {
			log.WithError(err).Fatal("Error creating Kinesis client")
		}

		svc := concept.NewService(s3Client, sqsClient, dynamoClient, kinesisClient, *neoWriterAddress, *elasticsearchWriterAddress, defaultHTTPClient())
		handler := concept.NewHandler(svc)
		hs := concept.NewHealthService(svc, *appSystemCode, *appName, *port, appDescription)

		router := mux.NewRouter()
		handler.RegisterHandlers(router)
		r := handler.RegisterAdminHandlers(router, hs, *requestLoggingOn)

		go svc.ListenForNotifications()

		log.Infof("Listening on port %v", *port)
		if err := http.ListenAndServe(":"+*port, r); err != nil {
			log.Fatalf("Unable to start server: %v", err)
		}
	}
	app.Run(os.Args)
}

func defaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 128,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
		},
	}
}
