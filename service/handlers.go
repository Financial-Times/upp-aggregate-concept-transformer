package service

import (
	"io/ioutil"
	"net/http"

	"encoding/json"
	"errors"
	"fmt"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	ut "github.com/Financial-Times/aggregate-concept-transformer/util"
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/Financial-Times/transactionid-utils-go"
	log "github.com/Sirupsen/logrus"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var keyMatcher = regexp.MustCompile("^[0-9a-f]{8}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{12}$")
var conceptWriterRoute = "__concepts-rw-neo4j/"
var elasticSearchRoute = "__concept-rw-elasticsearch/"

type AggregateConceptHandler struct {
	s3            s3.S3Driver
	sqs           sqs.SqsDriver
	vulcanAddress string
	httpClient    httpClient
}

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

func NewHandler(s3Driver s3.S3Driver, sqs sqs.SqsDriver, vulcanAddress string, httpClient httpClient) AggregateConceptHandler {
	return AggregateConceptHandler{
		s3:            s3Driver,
		sqs:           sqs,
		vulcanAddress: vulcanAddress,
		httpClient:    httpClient,
	}
}

func (h *AggregateConceptHandler) Run() {
	for {
		m := h.sqs.ListenAndServeQueue()
		if len(m) > 0 {
			h.ProcessMessages(m)
		}
	}
}

func (h *AggregateConceptHandler) ProcessMessages(messages []*awsSqs.Message) {
	numMessages := len(messages)
	var wg sync.WaitGroup
	wg.Add(numMessages)
	for i := range messages {
		go func(m *awsSqs.Message) {
			defer wg.Done()
			if err := h.processMessage(m); err != nil {
				log.Error(err.Error())
			}
		}(messages[i])
	}
}

func (h *AggregateConceptHandler) processMessage(message *awsSqs.Message) error {
	receiptHandle := message.ReceiptHandle
	sqsMessageBody := ut.Body{}
	messageAsBytes := []byte(*message.Body)
	err := json.Unmarshal(messageAsBytes, &sqsMessageBody)

	if err != nil {
		return err
	}

	updatedUuid, err := extractConceptUuidFromSqsMessage(sqsMessageBody.Message)
	if err != nil {
		return err
	}

	log.Infof("Processing message for concept with uuid: %s", updatedUuid)

	found, resp, tid, err := h.s3.GetConceptAndTransactionId(updatedUuid)
	if !found {
		if err != nil {
			return errors.New("Error retrieving concept: " + updatedUuid)
		}
		return errors.New("Concept not found: " + updatedUuid)
	}

	defer resp.Close()
	sourceConceptJson, err := ioutil.ReadAll(resp)
	sourceConceptModel := ut.SourceConceptJson{}
	err = json.Unmarshal(sourceConceptJson, &sourceConceptModel)
	if err != nil {
		return err
	}

	conceptType := resolveConceptType(strings.ToLower(sourceConceptModel.Type))

	concordedConcept, err := mapJson(sourceConceptModel, updatedUuid)
	if err != nil {
		return err
	}

	concordedJson, err := json.Marshal(concordedConcept)
	if err != nil {
		return err
	}

	//Write to Neo4j
	err = sendToWriter(h.httpClient, h.vulcanAddress+conceptWriterRoute, conceptType, updatedUuid, concordedJson, tid)
	if err != nil {
		return err
	}

	//Write to elastic search
	err = sendToWriter(h.httpClient, h.vulcanAddress+elasticSearchRoute, conceptType, updatedUuid, concordedJson, tid)
	if err != nil {
		return err
	}

	//Remove message from queue
	err = h.sqs.RemoveMessageFromQueue(receiptHandle)
	if err != nil {
		return err
	}

	log.Infof("Finished processing update of %s with transaction id %s", updatedUuid, tid)
	//TODO Dead Letter Queue?

	return nil
}

func (h *AggregateConceptHandler) GetHandler(rw http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	conceptUuid := vars["uuid"]

	//TODO check for concordance

	found, resp, tid, err := h.s3.GetConceptAndTransactionId(conceptUuid)
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

	defer resp.Close()
	b, err := ioutil.ReadAll(resp)
	if err != nil {
		log.Errorf("Error reading concept from buffer: %s", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("{\"message\":\"Error retrieving concept.\"}"))
		return
	}
	if tid == "" {
		log.Warnf("Concept %s did not have transaction id set in s3 metadata so one was generated", conceptUuid)
		tid = transactionidutils.GetTransactionIDFromRequest(r)
	}

	sourceConceptModel := ut.SourceConceptJson{}
	err = json.Unmarshal(b, &sourceConceptModel)
	if err != nil {
		log.Errorf("Unmarshaling of concept json resulted in error: %s", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusUnprocessableEntity)
		rw.Write([]byte("{\"message\":\"Invalid json returned from s3\"}"))
		return
	}

	concordedJson, err := mapJson(sourceConceptModel, conceptUuid)
	if err != nil {
		log.Errorf("Mapping of concorded json resulted in error: %s", err.Error())
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusUnprocessableEntity)
		rw.Write([]byte("{\"message\":\"Json from s3 cannot be concorded as it is missing key fields\"}"))
		return
	}

	output, err := json.Marshal(concordedJson)
	if err != nil {
		log.Errorf("Marshaling of concorded json resulted in error: %s", err.Error())
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("X-Request-Id", tid)
	rw.WriteHeader(http.StatusOK)
	rw.Write(output)
}

//Whilst this is not very nice; its a stop-gap until we properly tackle concordance
func mapJson(sourceConceptJson ut.SourceConceptJson, conceptUuid string) (ut.ConcordedConceptJson, error) {
	concordedConceptModel := ut.ConcordedConceptJson{}
	err := validateJson(sourceConceptJson, conceptUuid)
	if err != nil {
		return concordedConceptModel, err
	}
	sourceRep := ut.SourceRepresentation{}
	sourceReps := ut.SourceRepresentations{}
	sourceRep.UUID = sourceConceptJson.UUID
	sourceRep.PrefLabel = sourceConceptJson.PrefLabel
	sourceRep.Type = sourceConceptJson.Type
	sourceRep.Authority = "TME"
	for _, value := range sourceConceptJson.AlternativeIdentifiers.TME {

		sourceRep.AuthValue = value
	}
	concordedConceptModel.Type = sourceConceptJson.Type
	concordedConceptModel.UUID = sourceConceptJson.UUID
	concordedConceptModel.PrefLabel = sourceConceptJson.PrefLabel
	sourceReps = append(sourceReps, sourceRep)
	concordedConceptModel.SourceRepresentations = sourceReps
	return concordedConceptModel, nil
}

func validateJson(conceptJson ut.SourceConceptJson, conceptUuid string) error {
	if conceptJson.UUID == "" {
		return errors.New("Invalid Concept json for uuid " + conceptUuid + "; uuid field must not be blank")
	} else if conceptJson.PrefLabel == "" {
		return errors.New("Invalid Concept json for uuid " + conceptJson.UUID + "; prefLabel field must not be blank")
	} else if conceptJson.Type == "" {
		return errors.New("Invalid Concept json for uuid " + conceptJson.UUID + "; type field must not be blank")
	} else if len(conceptJson.AlternativeIdentifiers.TME) == 0 {
		return errors.New("Invalid Concept json for uuid " + conceptJson.UUID + "; must have source identifier field")
	} else {
		return nil
	}
}

func extractConceptUuidFromSqsMessage(sqsMessageBody string) (string, error) {
	sqsMessageRecord := ut.Message{}
	err := json.Unmarshal([]byte(sqsMessageBody), &sqsMessageRecord)
	if err != nil {
		return "", errors.New("Unmarshaling of concept json resulted in error: " + err.Error())
	}
	if sqsMessageRecord.Records == nil {
		return "", errors.New("Could not map message " + sqsMessageBody + " to expected json format")
	}
	key := sqsMessageRecord.Records[0].S3.Object.Key
	if key == "" {
		return "", errors.New("Could not extract concept uuid from message:" + sqsMessageBody)
	}

	if keyMatcher.MatchString(key) != true {
		return "", errors.New("Message key: " + key + ", is not a valid uuid")
	}
	return strings.Replace(key, "/", "-", 4), err
}

func sendToWriter(client httpClient, baseUrl string, urlParam string, conceptUuid string, body []byte, tid string) error {
	request, reqUrl, err := createWriteRequest(baseUrl, urlParam, strings.NewReader(string(body)), conceptUuid)
	if err != nil {
		return errors.New("Failed to create request to " + reqUrl + " with body " + string(body))
	}
	request.ContentLength = -1
	request.Header.Set("X-Request-Id", tid)

	resp, reqErr := client.Do(request)

	if reqErr != nil || resp.StatusCode != 200 {
		return errors.New("Request to " + reqUrl + " returned status: " + strconv.Itoa(resp.StatusCode) + "; skipping " + conceptUuid)
	}
	defer resp.Body.Close()

	return nil
}

func createWriteRequest(baseUrl string, urlParam string, msgBody io.Reader, uuid string) (*http.Request, string, error) {

	reqURL := baseUrl + urlParam + "/" + uuid

	request, err := http.NewRequest("PUT", reqURL, msgBody)
	if err != nil {
		return nil, reqURL, fmt.Errorf("Failed to create request to %s with body %s", reqURL, msgBody)
	}
	return request, reqURL, err
}

//Turn stored singular type to plural form
func resolveConceptType(conceptType string) string {
	var messageType string
	switch conceptType {
	case "person":
		messageType = "people"
	case "alphavilleseries":
		messageType = "alphaville-series"
	case "specialreport":
		messageType = "special-reports"
	default:
		messageType = conceptType + "s"
	}
	return messageType
}

func (h *AggregateConceptHandler) RegisterHandlers(router *mux.Router) {
	log.Info("Registering handlers")
	mh := handlers.MethodHandler{
		"GET": http.HandlerFunc(h.GetHandler),
	}
	router.Handle("/concept/{uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}}", mh)
}

func (h *AggregateConceptHandler) RegisterAdminHandlers(router *mux.Router) {
	log.Info("Registering admin handlers")
	var monitoringRouter http.Handler = router
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	var checks []v1a.Check = []v1a.Check{h.s3HealthCheck(), h.sqsHealthCheck(), h.conceptRwNeo4jHealthCheck(), h.conceptRwElasticSearchHealthCheck()}
	fmt.Print("We got here!\n")
	http.HandleFunc("/__health", v1a.Handler("ConceptIngester Healthchecks", "Checks for accessing writer", checks...))
	http.HandleFunc("/__gtg", h.gtgCheck)
	http.HandleFunc("/__ping", status.PingHandler)
	http.HandleFunc("/__build-info", status.BuildInfoHandler)
	http.Handle("/", monitoringRouter)
}

func (h *AggregateConceptHandler) checkConceptWriterAvailability() (string, error) {
	urlToCheck := h.vulcanAddress + conceptWriterRoute + "__gtg"
	resp, err := http.Get(urlToCheck)
	if err != nil {
		return "", fmt.Errorf("Error calling writer at %s : %v", urlToCheck, err)
	}
	resp.Body.Close()
	if resp != nil && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Writer %v returned status %d", urlToCheck, resp.StatusCode)
	}
	return "", nil
}

func (h *AggregateConceptHandler) checkElasticSearchWriterAvailability() (string, error) {
	urlToCheck := h.vulcanAddress + elasticSearchRoute + "__gtg"
	resp, err := http.Get(urlToCheck)
	if err != nil {
		return "", fmt.Errorf("Error calling writer at %s : %v", urlToCheck, err)
	}
	resp.Body.Close()
	if resp != nil && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Writer %v returned status %d", urlToCheck, resp.StatusCode)
	}
	return "", nil
}

func (h *AggregateConceptHandler) gtgCheck(rw http.ResponseWriter, r *http.Request) {
	if _, err := h.s3.HealthCheck(); err != nil {
		log.Errorf("S3 Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("S3 healthcheck failed"))
		return
	}
	if _, err := h.sqs.HealthCheck(); err != nil {
		log.Errorf("SQS Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("SQS healthcheck failed"))
		return
	}
	if _, err := h.checkConceptWriterAvailability(); err != nil {
		log.Errorf("Concept rw neo4j Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("Concept Rw Neo4j healthcheck failed"))
		return
	}
	if _, err := h.checkElasticSearchWriterAvailability(); err != nil {
		log.Errorf("Concept rw elastic search Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte("Elastic Search Rw healthcheck failed"))
		return
	}
	rw.WriteHeader(http.StatusOK)
}

func (h *AggregateConceptHandler) s3HealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to s3 bucket",
		Name:             "Check connectivity to s3 bucket",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/aggregate-concept-transformer",
		Severity:         1,
		TechnicalSummary: `Cannot connect to s3 bucket. If this check fails, check that amazon s3 is available`,
		Checker:          h.s3.HealthCheck,
	}
}

func (h *AggregateConceptHandler) sqsHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to sqs queue",
		Name:             "Check connectivity to sqs queue",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/aggregate-concept-transformer",
		Severity:         1,
		TechnicalSummary: `Cannot connect to sqs queue. If this check fails, check that amazon sqs s available`,
		Checker:          h.sqs.HealthCheck,
	}
}

func (h *AggregateConceptHandler) conceptRwNeo4jHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to concept writer neo4j",
		Name:             "Check connectivity to concept-rw-neo4j",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
		Severity:         1,
		TechnicalSummary: `Cannot connect to concept writer neo4j. If this check fails, check that the configured writer returns a healthy gtg`,
		Checker:          h.checkConceptWriterAvailability,
	}
}

func (h *AggregateConceptHandler) conceptRwElasticSearchHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to  elasticsearch concept writer",
		Name:             "Check connectivity to concept-rw-elasticsearch",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
		Severity:         1,
		TechnicalSummary: `Cannot connect to elasticsearch concept writer. If this check fails, check that the configured writer returns a healthy gtg`,
		Checker:          h.checkElasticSearchWriterAvailability,
	}
}
