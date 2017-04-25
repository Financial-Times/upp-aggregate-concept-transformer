package main

import (
	"net/http"
	"os"

	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/service"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	_ "github.com/joho/godotenv/autoload"
	"net"
	"time"
)

var httpClient = http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

func main() {

	app := cli.App("aggregate-concept-service", "Aggregating and concording concepts in UPP.")

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	awsRegion := app.String(cli.StringOpt{
		Name:   "awsRegion",
		Desc:   "AWS Region to connect to",
		EnvVar: "AWS_REGION",
	})

	bucketName := app.String(cli.StringOpt{
		Name:   "bucketName",
		Desc:   "Bucket to read concepts from.",
		EnvVar: "BUCKET_NAME",
	})

	queueUrl := app.String(cli.StringOpt{
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

	vulcanAddress := app.String(cli.StringOpt{
		Name:   "vulcanAddress",
		Value:  "http://localhost:8080/",
		Desc:   "Vulcan address for routing requests",
		EnvVar: "VULCAN_ADDR",
	})

	app.Action = func() {
		if *bucketName == "" {
			log.Fatal("S3 bucket name not set")
			return
		}
		if *queueUrl == "" {
			log.Fatal("SQS queue url not set")
			return
		}

		if *awsRegion == "" {
			log.Fatal("Aws Region not set")
		}

		s3Client, err := s3.NewClient(*bucketName, *awsRegion)
		if err != nil {
			log.Fatalf("Error creating S3 client: %v", err)
		}

		sqsClient, err := sqs.NewClient(*awsRegion, *queueUrl, *messagesToProcess, *visibilityTimeout, *waitTime)
		if err != nil {
			log.Fatalf("Error creating SQS client: %v", err)
		}

		router := mux.NewRouter()
		handler := service.NewHandler(s3Client, sqsClient, *vulcanAddress, &httpClient)
		handler.RegisterHandlers(router)
		handler.RegisterAdminHandlers(router)

		go handler.Run()

		log.Infof("Listening on %v", *port)
		if err := http.ListenAndServe(":"+*port, nil); err != nil {
			log.Fatalf("Unable to start server: %v", err)
		}
	}
	app.Run(os.Args)
}
