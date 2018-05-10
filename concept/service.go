package concept

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/Financial-Times/aggregate-concept-transformer/concordances"
	"github.com/Financial-Times/aggregate-concept-transformer/kinesis"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
)

const (
	smartlogicAuthority = "SmartLogic"
	thingsAPIEndpoint   = "/things"
)

var conceptTypesNotAllowedInElastic = [...]string{
	"Role",
	"Membership",
	"FinancialInstrument",
}

var irregularConceptTypePaths = map[string]string{
	"AlphavilleSeries": "alphaville-series",
	"BoardRole":        "membership-roles",
	"Dummy":            "dummies",
	"Person":           "people",
	"PublicCompany":    "organisations",
}

type Service interface {
	ListenForNotifications()
	ProcessMessage(UUID string) error
	GetConcordedConcept(UUID string) (ConcordedConcept, string, error)
	Healthchecks() []fthealth.Check
}

type AggregateService struct {
	s3                         s3.Client
	concordances               concordances.Client
	sqs                        sqs.Client
	kinesis                    kinesis.Client
	neoWriterAddress           string
	varnishPurgerAddress       string
	elasticsearchWriterAddress string
	httpClient                 httpClient
}

func NewService(S3Client s3.Client, SQSClient sqs.Client, concordancesClient concordances.Client, kinesisClient kinesis.Client, neoAddress string, elasticsearchAddress string, varnishPurgerAddress string, httpClient httpClient) Service {
	return &AggregateService{
		s3:                         S3Client,
		concordances:               concordancesClient,
		sqs:                        SQSClient,
		kinesis:                    kinesisClient,
		neoWriterAddress:           neoAddress,
		elasticsearchWriterAddress: elasticsearchAddress,
		varnishPurgerAddress:       varnishPurgerAddress,
		httpClient:                 httpClient,
	}
}

func (s *AggregateService) ListenForNotifications() {
	for {
		notifications := s.sqs.ListenAndServeQueue()
		if len(notifications) > 0 {
			var wg sync.WaitGroup
			wg.Add(len(notifications))
			for _, n := range notifications {
				go func(n sqs.Notification) {
					defer wg.Done()
					err := s.ProcessMessage(n.UUID)
					if err != nil {
						logger.WithError(err).WithUUID(n.UUID).Error("Error processing message.")
						return
					}
					err = s.sqs.RemoveMessageFromQueue(n.ReceiptHandle)
					if err != nil {
						logger.WithError(err).WithUUID(n.UUID).Error("Error removing message from SQS.")
					}
				}(n)
			}
			wg.Wait()
		}
	}
}

func (s *AggregateService) ProcessMessage(UUID string) error {
	// Get the concorded concept
	concordedConcept, transactionID, err := s.GetConcordedConcept(UUID)
	if err != nil {
		return err
	}
	if concordedConcept.PrefUUID != UUID {
		logger.WithTransactionID(transactionID).WithUUID(UUID).Infof("Requested concept %s is source node for canonical concept %s", UUID, concordedConcept.PrefUUID)
	}

	// Write to Neo4j
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debug("Sending concept to Neo4j")
	updatedConcepts, err := sendToWriter(s.httpClient, s.neoWriterAddress, resolveConceptType(concordedConcept.Type), concordedConcept.PrefUUID, concordedConcept, transactionID)
	if err != nil {
		return err
	} else if len(updatedConcepts.UpdatedIds) < 1 {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Info("Concept was unchanged since last update, skipping!")
		return nil
	}
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debug("Concept successfully updated in neo4j")

	// Purge concept URLs in varnish
	err = sendToPurger(s.httpClient, s.varnishPurgerAddress, updatedConcepts.UpdatedIds, concordedConcept.Type, transactionID)
	if err != nil {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("Concept couldn't be purged from Varnish cache")
	}

	// Write to Elasticsearch
	if isTypeAllowedInElastic(concordedConcept.Type) {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debug("Writing concept to elastic search")
		if _, err = sendToWriter(s.httpClient, s.elasticsearchWriterAddress, resolveConceptType(concordedConcept.Type), concordedConcept.PrefUUID, concordedConcept, transactionID); err != nil {
			return err
		}
	}

	conceptAsBytes, err := json.Marshal(updatedConcepts)
	if err != nil {
		logger.WithError(err).WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("Failed to marshall updatedIDs record: %v", updatedConcepts)
		return err
	}

	//Send notification to stream
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debugf("Sending notification of updated concepts to kinesis queue: %v", updatedConcepts)
	if err = s.kinesis.AddRecordToStream(conceptAsBytes, concordedConcept.Type); err != nil {
		logger.WithError(err).WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("Failed to update stream with notification record %v", updatedConcepts)
		return err
	}
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Infof("Finished processing update of %s", UUID)

	return nil
}

func bucketConcordances(concordances []concordances.ConcordanceRecord) (map[string][]string, string, error) {
	bucketedConcordances := map[string][]string{}
	for _, v := range concordances {
		bucketedConcordances[v.Authority] = append(bucketedConcordances[v.Authority], v.UUID)
	}

	// We hardcode primary authority to Smartlogic at the moment because that's the only source of
	// concordances.  This should change when we have multiple sources.  If there isn't one
	// (or there's more than one), we've gone horribly wrong.

	if concordances == nil || len(concordances) == 0 {
		err := fmt.Errorf("no concordances provided")
		logger.WithError(err).Error("Error grouping concordance records")
		return nil, "", err
	}

	if bucketedConcordances[smartlogicAuthority] != nil && len(bucketedConcordances[smartlogicAuthority]) != 1 {
		err := fmt.Errorf("only 1 primary authority allowed")
		logger.WithError(err).
			WithField("alert_tag", "AggregateConceptTransformerMultiplePrimaryAuthorities").
			WithField("primary_authorities", bucketedConcordances[smartlogicAuthority]).
			Error("Error grouping concordance records")
		return nil, "", err
	}

	return bucketedConcordances, smartlogicAuthority, nil
}

func (s *AggregateService) GetConcordedConcept(UUID string) (ConcordedConcept, string, error) {
	concordedConcept := ConcordedConcept{}
	// Get concordance UUIDs
	concordances, err := s.concordances.GetConcordance(UUID)
	if err != nil {
		return ConcordedConcept{}, "", err
	}
	logger.WithField("UUID", UUID).Debugf("Returned concordance record: %v", concordances)

	bucketedConcordances, primaryAuthority, err := bucketConcordances(concordances)
	if err != nil {
		return ConcordedConcept{}, "", err
	}

	// Get all concepts from S3
	for authority, authorityUUIDs := range bucketedConcordances {
		if authority == primaryAuthority {
			continue
		}
		for _, sourceId := range authorityUUIDs {
			found, sourceConcept, _, err := s.s3.GetConceptAndTransactionId(sourceId)
			if err != nil {
				return ConcordedConcept{}, "", err
			}

			if !found {
				err := fmt.Errorf("Source concept %s not found in S3", sourceId)
				logger.WithField("UUID", UUID).Error(err.Error())
				return ConcordedConcept{}, "", err
			}

			concordedConcept = mergeCanonicalInformation(concordedConcept, sourceConcept)
		}
	}

	canonicalConceptID := bucketedConcordances[primaryAuthority][0]

	found, primaryConcept, transactionID, err := s.s3.GetConceptAndTransactionId(canonicalConceptID)
	if err != nil {
		return ConcordedConcept{}, "", err
	} else if !found {
		err := fmt.Errorf("Canonical concept %s not found in S3", canonicalConceptID)
		logger.WithField("UUID", UUID).Error(err.Error())
		return ConcordedConcept{}, "", err
	}

	concordedConcept = mergeCanonicalInformation(concordedConcept, primaryConcept)
	concordedConcept.Aliases = deduplicateAliases(concordedConcept.Aliases)

	return concordedConcept, transactionID, nil
}

func (s *AggregateService) Healthchecks() []fthealth.Check {
	return []fthealth.Check{
		s.s3.Healthcheck(),
		s.sqs.Healthcheck(),
		s.RWElasticsearchHealthCheck(),
		s.RWNeo4JHealthCheck(),
		s.VarnishPurgerHealthCheck(),
		s.concordances.Healthcheck(),
		s.kinesis.Healthcheck(),
	}
}

func deduplicateAliases(aliases []string) []string {
	aMap := map[string]bool{}
	var outAliases []string
	for _, v := range aliases {
		aMap[v] = true
	}
	for a := range aMap {
		outAliases = append(outAliases, a)
	}
	return outAliases
}

func mergeCanonicalInformation(c ConcordedConcept, s s3.Concept) ConcordedConcept {
	c.PrefUUID = s.UUID
	c.PrefLabel = s.PrefLabel
	c.Type = s.Type
	c.Aliases = append(c.Aliases, s.Aliases...)
	c.Aliases = append(c.Aliases, s.PrefLabel)
	c.Strapline = s.Strapline
	c.DescriptionXML = s.DescriptionXML
	c.ImageURL = s.ImageURL
	c.EmailAddress = s.EmailAddress
	c.FacebookPage = s.FacebookPage
	c.TwitterHandle = s.TwitterHandle
	c.ScopeNote = s.ScopeNote
	c.ShortLabel = s.ShortLabel
	c.ParentUUIDs = s.ParentUUIDs
	c.BroaderUUIDs = s.BroaderUUIDs
	c.RelatedUUIDs = s.RelatedUUIDs
	c.SourceRepresentations = append(c.SourceRepresentations, s)

	if s.IsAuthor {
		c.IsAuthor = s.IsAuthor
	}

	for _, mr := range s.MembershipRoles {
		c.MembershipRoles = append(c.MembershipRoles, MembershipRole{
			RoleUUID:        mr.RoleUUID,
			InceptionDate:   mr.InceptionDate,
			TerminationDate: mr.TerminationDate,
		})
	}
	c.OrganisationUUID = s.OrganisationUUID
	c.PersonUUID = s.PersonUUID

	c.InceptionDate = s.InceptionDate
	c.TerminationDate = s.TerminationDate
	c.FigiCode = s.FigiCode
	c.IssuedBy = s.IssuedBy

	return c
}

func sendToPurger(client httpClient, baseUrl string, conceptUUIDs []string, conceptType string, tid string) error {

	req, err := http.NewRequest("POST", strings.TrimRight(baseUrl, "/")+"/purge", nil)
	if err != nil {
		return err
	}

	queryParams := req.URL.Query()
	for _, cUUID := range conceptUUIDs {
		queryParams.Add("target", thingsAPIEndpoint+"/"+cUUID)
	}

	// purge the public endpoints as well (/people, /brands and /organisations)
	if conceptType == "Person" || conceptType == "Brand" || conceptType == "Organisation" || conceptType == "PublicCompany" {
		urlParam := resolveConceptType(conceptType)
		for _, cUUID := range conceptUUIDs {
			queryParams.Add("target", "/"+urlParam+"/"+cUUID)
		}
	}

	req.URL.RawQuery = queryParams.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Request was not successful, status code: %v", resp.StatusCode)
	}

	if err == nil {
		logger.WithTransactionID(tid).Debugf("Concepts with ids %s successfully purged from varnish cache", conceptUUIDs)
	}

	return err
}

func sendToWriter(client httpClient, baseUrl string, urlParam string, conceptUUID string, concept ConcordedConcept, tid string) (UpdatedConcepts, error) {
	updatedConcepts := UpdatedConcepts{}
	body, err := json.Marshal(concept)
	if err != nil {
		return updatedConcepts, err
	}

	request, reqUrl, err := createWriteRequest(baseUrl, urlParam, strings.NewReader(string(body)), conceptUUID)
	if err != nil {
		err := errors.New("Failed to create request to " + reqUrl + " with body " + string(body))
		logger.WithTransactionID(tid).WithUUID(conceptUUID).Error(err)
		return updatedConcepts, err
	}
	request.ContentLength = -1
	request.Header.Set("X-Request-Id", tid)
	resp, reqErr := client.Do(request)

	defer resp.Body.Close()

	if strings.Contains(baseUrl, "neo4j") && int(resp.StatusCode/100) == 2 {
		dec := json.NewDecoder(resp.Body)
		if err = dec.Decode(&updatedConcepts); err != nil {
			logger.WithError(err).WithTransactionID(tid).WithUUID(conceptUUID).Error("Error whilst decoding response from writer")
			return updatedConcepts, err
		}
	}

	if resp.StatusCode == 404 && strings.Contains(baseUrl, "elastic") {
		logger.WithTransactionID(tid).WithUUID(conceptUUID).Debugf("Elastic search rw cannot handle concept: %s, because it has an unsupported type %s; skipping record", conceptUUID, concept.Type)
		return updatedConcepts, nil
	} else if reqErr != nil || resp.StatusCode != 200 {
		err := errors.New("Request to " + reqUrl + " returned status: " + strconv.Itoa(resp.StatusCode) + "; skipping " + conceptUUID)
		logger.WithError(reqErr).WithTransactionID(tid).WithUUID(conceptUUID).Errorf("Request to %s returned status: %d", reqUrl, resp.StatusCode)
		return updatedConcepts, err
	}

	return updatedConcepts, nil
}

func createWriteRequest(baseUrl string, urlParam string, msgBody io.Reader, uuid string) (*http.Request, string, error) {

	reqURL := strings.TrimRight(baseUrl, "/") + "/" + urlParam + "/" + uuid

	request, err := http.NewRequest("PUT", reqURL, msgBody)
	if err != nil {
		return nil, reqURL, fmt.Errorf("failed to create request to %s with body %s", reqURL, msgBody)
	}
	return request, reqURL, err
}

//Turn stored singular type to plural form
func resolveConceptType(conceptType string) string {
	if ipath, ok := irregularConceptTypePaths[conceptType]; ok && ipath != "" {
		return ipath
	}

	return toSnakeCase(conceptType) + "s"
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}-${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}-${2}")
	return strings.ToLower(snake)
}

func (s *AggregateService) RWNeo4JHealthCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to concept-rw-neo4j",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         3,
		TechnicalSummary: `Cannot connect to concept writer neo4j. If this check fails, check health of concepts-rw-neo4j service`,
		Checker: func() (string, error) {
			urlToCheck := strings.TrimRight(s.neoWriterAddress, "/") + "/__gtg"
			req, err := http.NewRequest("GET", urlToCheck, nil)
			if err != nil {
				return "", err
			}
			resp, err := s.httpClient.Do(req)
			if err != nil {
				return "", fmt.Errorf("error calling writer at %s : %v", urlToCheck, err)
			}
			resp.Body.Close()
			if resp != nil && resp.StatusCode != http.StatusOK {
				return "", fmt.Errorf("writer %v returned status %d", urlToCheck, resp.StatusCode)
			}
			return "", nil
		},
	}
}

func (s *AggregateService) VarnishPurgerHealthCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts won't be immediately refreshed in the cache",
		Name:             "Check connectivity to varnish purger",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         3,
		TechnicalSummary: `Cannot connect to varnish purger. If this check fails, check health of varnish-purger service`,
		Checker: func() (string, error) {
			urlToCheck := strings.TrimRight(s.varnishPurgerAddress, "/") + "/__gtg"
			req, err := http.NewRequest("GET", urlToCheck, nil)
			if err != nil {
				return "", err
			}
			resp, err := s.httpClient.Do(req)
			if err != nil {
				return "", fmt.Errorf("error calling purger at %s : %v", urlToCheck, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return "", fmt.Errorf("purger %v returned status %d", urlToCheck, resp.StatusCode)
			}
			return "", nil
		},
	}
}

func (s *AggregateService) RWElasticsearchHealthCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Editorial updates of concepts will not be written into UPP",
		Name:             "Check connectivity to concept-rw-elasticsearch",
		PanicGuide:       "https://dewey.ft.com/aggregate-concept-transformer.html",
		Severity:         3,
		TechnicalSummary: `Cannot connect to elasticsearch concept writer. If this check fails, check health of concept-rw-elasticsearch service`,
		Checker: func() (string, error) {
			urlToCheck := strings.TrimRight(s.elasticsearchWriterAddress, "/bulk") + "/__gtg"
			req, err := http.NewRequest("GET", urlToCheck, nil)
			if err != nil {
				return "", err
			}
			resp, err := s.httpClient.Do(req)
			if err != nil {
				return "", fmt.Errorf("error calling writer at %s : %v", urlToCheck, err)
			}
			resp.Body.Close()
			if resp != nil && resp.StatusCode != http.StatusOK {
				return "", fmt.Errorf("writer %v returned status %d", urlToCheck, resp.StatusCode)
			}
			return "", nil
		},
	}
}

func isTypeAllowedInElastic(conceptType string) bool {
	for _, ct := range conceptTypesNotAllowedInElastic {
		if ct == conceptType {
			return false
		}
	}

	return true
}
