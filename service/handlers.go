package service

import (
	"io/ioutil"
	"net/http"

	"github.com/Financial-Times/aggregate-concept-transformer/kafka"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"time"
	"github.com/Financial-Times/transactionid-utils-go"
	"encoding/json"
)

type AggregateConceptHandler struct {
	s3    s3.Client
	kafka kafka.Client
}

func NewHandler(s3Client s3.Client, kafka kafka.Client) AggregateConceptHandler {
	return AggregateConceptHandler{
		s3:    s3Client,
		kafka: kafka,
	}
}

func (h *AggregateConceptHandler) GetHandler(rw http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uuid := vars["uuid"]

	found, rc, err := h.s3.GetConcept(uuid)
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

	b, err := ioutil.ReadAll(rc)
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

	concept := Concept{}
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
	}
	message := kafka.FTMessage{Headers: buildHeader(conceptUuid, concept.Type, tid), Body: string(body)}

	h.kafka.SendMessage(message.String())

	defer resp.Close()

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusAccepted)
	rw.Write([]byte("{\"message\":\"Concept published to queue\"}"))
}

func buildHeader(conceptUuid string, conceptType string, tid string) map[string]string {
	return map[string]string{
		"Message-Id":        conceptUuid,
		"Message-Type":      conceptType,
		"Content-Type":      "application/json",
		"X-Request-Id":      tid,
		"Origin-System-Id":  "http://cmdb.ft.com/systems/upp/rds", //TODO IS this right?
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
