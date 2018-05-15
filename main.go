package main

import (
	"net/http"
	"os"

	"net"
	"time"

	"github.com/Financial-Times/aggregate-concept-transformer/concept"
	"github.com/Financial-Times/aggregate-concept-transformer/concordances"
	"github.com/Financial-Times/aggregate-concept-transformer/kinesis"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	"github.com/Financial-Times/go-logger"
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
	concordancesReaderAddress := app.String(cli.StringOpt{
		Name:   "concordancesReaderAddress",
		Value:  "http://localhost:8080/",
		Desc:   "Address for the Neo4J Concept Writer",
		EnvVar: "CONCORDANCES_RW_ADDRESS",
	})
	elasticsearchWriterAddress := app.String(cli.StringOpt{
		Name:   "elasticsearchWriterAddress",
		Value:  "http://localhost:8080/",
		Desc:   "Address for the Elasticsearch Concept Writer",
		EnvVar: "ES_WRITER_ADDRESS",
	})
	varnishPurgerAddress := app.String(cli.StringOpt{
		Name:   "varnishPurgerAddress",
		Value:  "http://localhost:8080/",
		Desc:   "Address for the Varnish Purger application",
		EnvVar: "VARNISH_PURGER_ADDRESS",
	})
	typesToPurgeFromPublicEndpoints := app.Strings(cli.StringsOpt{
		Name:   "typesToPurgeFromPublicEndpoints",
		Value:  []string{"Person", "Brand", "Organisation", "PublicCompany"},
		Desc:   "Concept types that need purging from specific public endpoints (other than /things)",
		EnvVar: "TYPES_TO_PURGE_FROM_PUBLIC_ENDPOINTS",
	})
	crossAccountRoleARN := app.String(cli.StringOpt{
		Name:      "crossAccountRoleARN",
		HideValue: true,
		Desc:      "ARN for cross account role",
		EnvVar:    "CROSS_ACCOUNT_ARN",
	})
	kinesisStreamName := app.String(cli.StringOpt{
		Name:   "kinesisStreamName",
		Desc:   "AWS Kinesis stream name",
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

		logger.InitLogger(*appSystemCode, *logLevel)

		logger.WithFields(log.Fields{
			"ES_WRITER_ADDRESS":       *elasticsearchWriterAddress,
			"CONCORDANCES_RW_ADDRESS": *concordancesReaderAddress,
			"NEO_WRITER_ADDRESS":      *neoWriterAddress,
			"VARNISH_PURGER_ADDRESS":  *varnishPurgerAddress,
			"BUCKET_REGION":           *bucketRegion,
			"BUCKET_NAME":             *bucketName,
			"SQS_REGION":              *sqsRegion,
			"QUEUE_URL":               *queueURL,
			"LOG_LEVEL":               *logLevel,
			"KINESIS_STREAM_NAME":     *kinesisStreamName,
		}).Info("Starting app with arguments")

		if *bucketName == "" {
			logger.Fatal("S3 bucket name not set")
		}

		if *queueURL == "" {
			logger.Fatal("SQS queue url not set")
		}

		if *bucketRegion == "" {
			logger.Fatal("AWS bucket region not set")
		}

		if *sqsRegion == "" {
			logger.Fatal("AWS SQS region not set")
		}

		if *kinesisStreamName == "" {
			logger.Fatal("Kinesis stream name not set")
		}

		if *concordancesReaderAddress == "" {
			logger.Fatal("Concordances reader address not set")
		}

		s3Client, err := s3.NewClient(*bucketName, *bucketRegion)
		if err != nil {
			logger.WithError(err).Fatal("Error creating S3 client")
		}

		sqsClient, err := sqs.NewClient(*sqsRegion, *queueURL, *messagesToProcess, *visibilityTimeout, *waitTime)
		if err != nil {
			logger.WithError(err).Fatal("Error creating SQS client")
		}

		concordancesClient, err := concordances.NewClient(*concordancesReaderAddress)
		if err != nil {
			logger.WithError(err).Fatal("Error creating Concordances client")
		}

		kinesisClient, err := kinesis.NewClient(*kinesisStreamName, *kinesisRegion, *crossAccountRoleARN)
		if err != nil {
			logger.WithError(err).Fatal("Error creating Kinesis client")
		}

		svc := concept.NewService(s3Client, sqsClient, concordancesClient, kinesisClient, *neoWriterAddress, *elasticsearchWriterAddress, *varnishPurgerAddress, *typesToPurgeFromPublicEndpoints, defaultHTTPClient())
		handler := concept.NewHandler(svc)
		hs := concept.NewHealthService(svc, *appSystemCode, *appName, *port, appDescription)

		router := mux.NewRouter()
		handler.RegisterHandlers(router)
		r := handler.RegisterAdminHandlers(router, hs, *requestLoggingOn)

		go svc.ListenForNotifications()

		logger.Infof("Listening on port %v", *port)
		if err := http.ListenAndServe(":"+*port, r); err != nil {
			logger.Fatalf("Unable to start server: %v", err)
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
