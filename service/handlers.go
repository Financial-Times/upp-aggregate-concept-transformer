package service

import (
	"io/ioutil"
	"net/http"

	"encoding/json"
	"github.com/Financial-Times/aggregate-concept-transformer/kafka"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	ut "github.com/Financial-Times/aggregate-concept-transformer/util"
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"strings"
	"time"
)

type AggregateConceptHandler struct {
	s3    s3.S3Driver
	kafka kafka.Client
}

func NewHandler(s3Driver s3.S3Driver, kafka kafka.Client) AggregateConceptHandler {
	return AggregateConceptHandler{
		s3:    s3Driver,
		kafka: kafka,
	}
}

func (h *AggregateConceptHandler) GetHandler(rw http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	found, resp, err := h.s3.GetConcept(uuid)
	if !found {
		if err != nil {
			log.Errorf("Error retrieving concept: %s", err.Error())
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte("{\"message\":\"Error retrieving concept.\"}"))
			return
		}
		log.Errorf("Concept not found: %s", uuid)
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Concept not found.\"}"))
		return
	}

	b, err := ioutil.ReadAll(resp)
	if err != nil {
		log.Errorf("Error reading concept from buffer: %s", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("{\"message\":\"Error retrieving concept.\"}"))
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	rw.Write(b)
}

func (h *AggregateConceptHandler) PostHandler(rw http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conceptUuid := vars["uuid"]
	tid := transactionidutils.GetTransactionIDFromRequest(r)

	found, resp, err := h.s3.GetConcept(conceptUuid)
	if !found {
		if err != nil {
			log.Errorf("Error retrieving concept: %s", err.Error())
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte("{\"message\":\"Error retrieving concept.\"}"))
			return
		}
		log.Errorf("Concept not found: %s", conceptUuid)
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Concept not found.\"}"))
		return
	}

	concept := ut.Concept{}
	body, err := ioutil.ReadAll(resp)
	if err != nil {
		log.Errorf("Error reading concept from buffer: %s", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("{\"message\":\"Error retrieving concept.\"}"))
		return
	}
	err = json.Unmarshal(body, &concept)
	if err != nil {
		log.Errorf("Could not unmarshall data from s3: %s into valid json", string(body))
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusUnprocessableEntity)
		rw.Write([]byte("{\"message\":\"Retrived concept is invalid json.\"}"))
		return
	}
	message := kafka.FTMessage{Headers: buildHeader(conceptUuid, resolveMessageType(strings.ToLower(concept.Type)), tid), Body: string(body)}

	partition, offset, err := h.kafka.Producer.SendMessage(&sarama.ProducerMessage{Topic: h.kafka.Topic, Value: sarama.StringEncoder(message.String())})

	log.Infof("Message id %s written to %s topic on partition %d with an offset of %d", conceptUuid, h.kafka.Topic, partition, offset)

	defer resp.Close()

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusAccepted)
	rw.Write([]byte("{\"message\":\"Concept published to queue\"}"))
}

//Current implementation of concept-ingester(service which consumes these messages) uses the message-type header to resolve request url; hence need for "pluralisation"
func resolveMessageType(conceptType string) string {
	var messageType string
	if conceptType == "person" {
		messageType = "people"
	} else {
		messageType = conceptType + "s"
	}
	return messageType
 }

func buildHeader(conceptUuid string, messageType string, tid string) map[string]string {
	return map[string]string{
		"Message-Id":        conceptUuid,
		"Message-Type":      messageType,
		"Content-Type":      "application/json",
		"X-Request-Id":      tid,
		"Origin-System-Id":  "aggregate-concept-transformer",
		"Message-Timestamp": time.Now().Format("2006-01-02T15:04:05.000Z"),
	}
}

func (h *AggregateConceptHandler) RegisterAdminHandlers(router *mux.Router) {
	log.Info("Registering admin handlers")
	var monitoringRouter http.Handler = router
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc("/__health", v1a.Handler("GenericReadWriteS3 Healthchecks",
		"Runs a HEAD check on bucket", v1a.Check{
			BusinessImpact:   "Unable to access S3 bucket",
			Name:             "S3 Bucket check",
			PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
			Severity:         1,
			TechnicalSummary: `Can not access S3 bucket.`,
			Checker:          h.s3.HealthCheck,
		}))
	http.HandleFunc("/__gtg", h.gtgCheck)
	http.Handle("/", monitoringRouter)
}

func (h *AggregateConceptHandler) RegisterHandlers(router *mux.Router) {
	log.Info("Registering handlers")
	mh := handlers.MethodHandler{
		"GET":  http.HandlerFunc(h.GetHandler),
		"POST": http.HandlerFunc(h.PostHandler),
	}
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", mh)
}

func (h *AggregateConceptHandler) gtgCheck(rw http.ResponseWriter, r *http.Request) {
	resp, err := h.s3.HealthCheck()
	if err != nil {
		log.Errorf("S3 Healthcheck failed (%s): %v", resp, err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	rw.WriteHeader(http.StatusOK)
}
