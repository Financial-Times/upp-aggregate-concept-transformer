package main

import (
	"net/http"
	"os"

	"github.com/Financial-Times/aggregate-concept-transformer/kafka"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/service"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	_ "github.com/joho/godotenv/autoload"
)

func main() {

	app := cli.App("aggregate-concept-service", "Aggregating and concording concepts in UPP.")

	bucketName := app.String(cli.StringOpt{
		Name:   "bucketName",
		Desc:   "Bucket to read concepts from.",
		EnvVar: "BUCKET_NAME",
	})

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	topic := app.String(cli.StringOpt{
		Name:   "topic",
		Value:  "Concept",
		Desc:   "Kafka topic to write the concept to",
		EnvVar: "KAFKA_TOPIC",
	})

	zkAddresses := app.Strings(cli.StringsOpt{
		Name:   "zkAddresses",
		Desc:   "Zookeeper addresses to connect to",
		EnvVar: "ZK_ADDRESSES",
	})

	app.Action = func() {

		s3Client, err := s3.NewClient(*bucketName)
		if err != nil {
			log.Fatalf("Error creating S3 client: %v", err)
		}

		kafka, err := kafka.NewClient(*zkAddresses, *topic)
		if err != nil {
			log.Fatalf("Error creating Kafka client: %v", err)
		}

		router := mux.NewRouter()
		handler := service.NewHandler(s3Client, kafka)
		handler.RegisterHandlers(router)
		handler.RegisterAdminHandlers(router)

		log.Infof("Listening on %v", *port)
		if err := http.ListenAndServe(":"+*port, nil); err != nil {
			log.Fatalf("Unable to start server: %v", err)
		}
	}

	app.Run(os.Args)
}
