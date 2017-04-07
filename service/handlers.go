package service

import (
	"io/ioutil"
	"net/http"

	"encoding/json"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	ut "github.com/Financial-Times/aggregate-concept-transformer/util"
	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"time"
	"sync"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"io"
	"net"
	"strconv"
)

var keyMatcher = regexp.MustCompile("^[0-9a-f]{8}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{4}/[0-9a-f]{12}$")
var conceptWriterRoute = "__concepts-rw-neo4j/"
var elasticSearchRoute = "__concept-rw-elasticsearch/"

var httpClient = http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

type AggregateConceptHandler struct {
	s3    s3.S3Driver
	sqs   sqs.SqsDriver
	vulcanAddress string
}

func NewHandler(s3Driver s3.S3Driver, sqs sqs.SqsDriver, vulcanAddress string) AggregateConceptHandler {
	return AggregateConceptHandler{
		s3:    s3Driver,
		sqs: sqs,
		vulcanAddress: vulcanAddress,
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

func  (h *AggregateConceptHandler) ProcessMessages(messages []*awsSqs.Message) {
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

func  (h *AggregateConceptHandler) processMessage(message *awsSqs.Message) error {
	receiptHandle := message.ReceiptHandle
	sqsMessageBody := ut.Body{}
	err := json.Unmarshal([]byte(*message.Body), &sqsMessageBody)
	if err != nil {
		return err
	}
	updatedUuid, err := extractConceptUuidFromSqsMessage(sqsMessageBody.Message)
	if err != nil {
		return err
	}

	//Check for concordance and do it
	checkForConcordances(updatedUuid)

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
	conceptType := resolveConceptType(strings.ToLower(sourceConceptModel.Type))

	concordedJson := mapJson(sourceConceptModel)
	result, err := json.Marshal(concordedJson)

	//Write to Neo4j
	err = sendToWriter(h.vulcanAddress + conceptWriterRoute, conceptType, updatedUuid, result, tid)
	if err != nil {
		return err
	}

	//Write to elastic search
	err = sendToWriter(h.vulcanAddress + elasticSearchRoute, conceptType, updatedUuid, result, tid)
	if err != nil {
		return err
	}

	//Remove message from queue
	err = h.sqs.RemoveMessageFromQueue(receiptHandle)
	if err != nil {
		return err
	}

	//TODO Dead Letter Queue?

	return nil
}
func mapJson(sourceConceptJson ut.SourceConceptJson) ut.ConcordedConceptJson {
	//This is not very nice but in it will have to do in lieu of real concordance
	concordedConceptModel := ut.ConcordedConceptJson{}
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
	return concordedConceptModel
}
func checkForConcordances(updatedUuid string) []string {
	//Should check for concordance and return list of uuids
	var uuidList []string
	uuidList = append(uuidList, updatedUuid)
	return uuidList
}

func extractConceptUuidFromSqsMessage(sqsMessageBody string) (string, error){
	sqsMessageRecord := ut.Message{}
	err := json.Unmarshal([]byte(sqsMessageBody), &sqsMessageRecord)
	if err != nil {
		return "", err
	}
	key := sqsMessageRecord.Records[0].S3.Object.Key
	if keyMatcher.MatchString(key) != true {
		return "", errors.New("Message key: " + key + " was not expected format")
	}
	return strings.Replace(key, "/", "-", 4), err
}

func sendToWriter(baseUrl string, urlParam string, conceptUuid string, body []byte, tid string) (error) {
	request, reqUrl, err := createWriteRequest(baseUrl, urlParam, strings.NewReader(string(body)), conceptUuid)
	if err != nil {
		return errors.New("Failed to create request to " + reqUrl + " with body " + string(body))
	}
	request.ContentLength = -1
	request.Header.Set("X-Request-Id", tid)

	resp, reqErr := httpClient.Do(request)
	if reqErr != nil {
		return errors.New("Request to writer " + reqUrl + " for uuid " + conceptUuid + " failed with error " + reqErr.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.New("Request to " + reqUrl + " returned status " + strconv.Itoa(resp.StatusCode) + "; skipping " + conceptUuid)
	}
	return nil
}

func createWriteRequest(baseUrl string, urlParam string, msgBody io.Reader, uuid string) (*http.Request, string, error) {

	reqURL := baseUrl + urlParam + "/" + uuid

	request, err := http.NewRequest("PUT", reqURL, msgBody)
	if err != nil {
		return nil, reqURL, fmt.Errorf("Failed to create request to %v with body %v", reqURL, msgBody)
	}
	return request, reqURL, err
}

//Turn stored singular type to plural form
func resolveConceptType(conceptType string) string {
	var messageType string
	if conceptType == "person" {
		messageType = "people"
	} else {
		messageType = conceptType + "s"
	}
	return messageType
}

func (h *AggregateConceptHandler) RegisterAdminHandlers(router *mux.Router) {
	log.Info("Registering admin handlers")
	var monitoringRouter http.Handler = router
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)

	var checks []v1a.Check = []v1a.Check{h.s3HealthCheck(), h.sqsHealthCheck()} //, h.conceptRwNeo4jHealthCheck(), h.conceptRwElasticSearchHealthCheck()}
	http.HandleFunc("/__health", v1a.Handler("ConceptIngester Healthchecks", "Checks for accessing writer", checks...))
	http.HandleFunc("/__gtg", h.gtgCheck)
	//http.HandleFunc("/__build-info", h.buildInfo)
	http.Handle("/", monitoringRouter)
}

func (h *AggregateConceptHandler) checkWriterAvailability(route string) (string, error) {
	urlToCheck := h.vulcanAddress + route + "__gtg"
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
		return
	}
	if _, err := h.sqs.HealthCheck(); err != nil {
		log.Errorf("SQS Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if _, err := h.checkWriterAvailability(conceptWriterRoute); err != nil {
		log.Errorf("Concept rw neo4j Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if _, err := h.checkWriterAvailability(elasticSearchRoute); err != nil {
		log.Errorf("Concept rw elastic search Healthcheck failed; %v", err.Error())
		rw.WriteHeader(http.StatusServiceUnavailable)
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

//func (h *AggregateConceptHandler) conceptRwNeo4jHealthCheck() v1a.Check {
//	return v1a.Check{
//		BusinessImpact:   "Unable to connect to concept writer neo4j",
//		Name:             "Check connectivity to concept-rw-neo4j",
//		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
//		Severity:         1,
//		TechnicalSummary: `Cannot connect to concept writer neo4j. If this check fails, check that the configured writer returns a healthy gtg`,
//		Checker:          h.checkWriterAvailability(conceptWriterRoute),
//	}
//}
//
//func (h *AggregateConceptHandler) conceptRwElasticSearchHealthCheck() v1a.Check {
//	return v1a.Check{
//		BusinessImpact:   "Unable to connect to  elasticsearch concept writer",
//		Name:             "Check connectivity to concept-rw-elasticsearch",
//		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
//		Severity:         1,
//		TechnicalSummary: `Cannot connect to elasticsearch concept writer. If this check fails, check that the configured writer returns a healthy gtg`,
//		Checker:          h.checkWriterAvailability(elasticSearchRoute),
//	}
//}
