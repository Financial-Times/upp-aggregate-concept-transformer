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
	smartlogicAuthority      = "Smartlogic"
	managedLocationAuthority = "ManagedLocation"
	thingsAPIEndpoint        = "/things"
	conceptsAPIEnpoint       = "/concepts"
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
	s3                              s3.Client
	concordances                    concordances.Client
	conceptUpdatesSqs               sqs.Client
	eventsSqs                       sqs.Client
	kinesis                         kinesis.Client
	neoWriterAddress                string
	varnishPurgerAddress            string
	elasticsearchWriterAddress      string
	httpClient                      httpClient
	typesToPurgeFromPublicEndpoints []string
}

func NewService(S3Client s3.Client, conceptUpdatesSQSClient sqs.Client, eventsSQSClient sqs.Client, concordancesClient concordances.Client, kinesisClient kinesis.Client, neoAddress string, elasticsearchAddress string, varnishPurgerAddress string, typesToPurgeFromPublicEndpoints []string, httpClient httpClient) Service {
	return &AggregateService{
		s3:                              S3Client,
		concordances:                    concordancesClient,
		conceptUpdatesSqs:               conceptUpdatesSQSClient,
		eventsSqs:                       eventsSQSClient,
		kinesis:                         kinesisClient,
		neoWriterAddress:                neoAddress,
		elasticsearchWriterAddress:      elasticsearchAddress,
		varnishPurgerAddress:            varnishPurgerAddress,
		httpClient:                      httpClient,
		typesToPurgeFromPublicEndpoints: typesToPurgeFromPublicEndpoints,
	}
}

func (s *AggregateService) ListenForNotifications() {
	for {
		notifications := s.conceptUpdatesSqs.ListenAndServeQueue()
		if len(notifications) > 0 {
			var wg sync.WaitGroup
			wg.Add(len(notifications))
			for _, n := range notifications {
				go func(n sqs.ConceptUpdate) {
					defer wg.Done()
					err := s.ProcessMessage(n.UUID)
					if err != nil {
						logger.WithError(err).WithUUID(n.UUID).Error("Error processing message.")
						return
					}
					err = s.conceptUpdatesSqs.RemoveMessageFromQueue(n.ReceiptHandle)
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
	conceptChanges, err := sendToWriter(s.httpClient, s.neoWriterAddress, resolveConceptType(concordedConcept.Type), concordedConcept.PrefUUID, concordedConcept, transactionID)
	if err != nil {
		return err
	}
	rawJson, err := json.Marshal(conceptChanges)
	if err != nil {
		logger.WithError(err).WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("failed to marshall concept changes record: %v", conceptChanges)
		return err
	}
	var updateRecord sqs.ConceptChanges
	if err = json.Unmarshal(rawJson, &updateRecord); err != nil {
		logger.WithError(err).WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("failed to unmarshall raw json into update record: %v", rawJson)
		return err
	}

	if len(updateRecord.ChangedRecords) < 1 {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Info("concept was unchanged since last update, skipping!")
		return nil
	}
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debug("concept successfully updated in neo4j")

	// Purge concept URLs in varnish
	// Always purge top level concept
	err = sendToPurger(s.httpClient, s.varnishPurgerAddress, updateRecord.UpdatedIds, concordedConcept.Type, s.typesToPurgeFromPublicEndpoints, transactionID)
	if err != nil {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("Concept couldn't be purged from Varnish cache")
	}

	//optionally purge other affected concepts
	if concordedConcept.Type == "FinancialInstrument" {
		err = sendToPurger(s.httpClient, s.varnishPurgerAddress, []string{concordedConcept.SourceRepresentations[0].IssuedBy}, "Organisation", s.typesToPurgeFromPublicEndpoints, transactionID)
	}
	if err != nil {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.SourceRepresentations[0].IssuedBy).Errorf("Concept couldn't be purged from Varnish cache")
	}

	if concordedConcept.Type == "Membership" {
		err = sendToPurger(s.httpClient, s.varnishPurgerAddress, []string{concordedConcept.PersonUUID}, "Person", s.typesToPurgeFromPublicEndpoints, transactionID)
	}
	if err != nil {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PersonUUID).Errorf("Concept couldn't be purged from Varnish cache")
	}

	// Write to Elasticsearch
	if isTypeAllowedInElastic(concordedConcept.Type) {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debug("Writing concept to elastic search")
		if _, err = sendToWriter(s.httpClient, s.elasticsearchWriterAddress, resolveConceptType(concordedConcept.Type), concordedConcept.PrefUUID, concordedConcept, transactionID); err != nil {
			return err
		}
	}

	if err := s.eventsSqs.SendEvents(updateRecord.ChangedRecords); err != nil {
		logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PersonUUID).Errorf("unable to send events: %v to Event Queue", updateRecord.ChangedRecords)
		return err
	}

	//Send notification to stream
	rawIDList, err := json.Marshal(conceptChanges.UpdatedIds)
	if err != nil {
		logger.WithError(err).WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("failed to marshall concept changes record: %v", conceptChanges.UpdatedIds)
		return err
	}
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Debugf("sending notification of updated concepts to kinesis conceptsQueue: %v", conceptChanges)
	if err = s.kinesis.AddRecordToStream(rawIDList, concordedConcept.Type); err != nil {
		logger.WithError(err).WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Errorf("Failed to update stream with notification record %v", conceptChanges)
		return err
	}
	logger.WithTransactionID(transactionID).WithUUID(concordedConcept.PrefUUID).Infof("Finished processing update of %s", UUID)

	return nil
}

func bucketConcordances(concordanceRecords []concordances.ConcordanceRecord) (map[string][]concordances.ConcordanceRecord, string, error) {
	if concordanceRecords == nil || len(concordanceRecords) == 0 {
		err := fmt.Errorf("no concordances provided")
		logger.WithError(err).Error("Error grouping concordance records")
		return nil, "", err
	}

	bucketedConcordances := map[string][]concordances.ConcordanceRecord{}
	for _, v := range concordanceRecords {
		bucketedConcordances[v.Authority] = append(bucketedConcordances[v.Authority], v)
	}

	var primaryAuthority string
	var err error
	slRecords, slFound := bucketedConcordances[smartlogicAuthority]
	if slFound {
		if len(slRecords) == 1 {
			primaryAuthority = smartlogicAuthority
		} else {
			err = fmt.Errorf("more than 1 primary authority")
		}
	}
	mlRecords, mlFound := bucketedConcordances[managedLocationAuthority]
	if mlFound {
		if len(mlRecords) == 1 {
			if primaryAuthority == "" {
				primaryAuthority = managedLocationAuthority
			}
		} else {
			err = fmt.Errorf("more than 1 ManagedLocation primary authority")
		}
	}
	if err != nil {
		logger.WithError(err).
			WithField("alert_tag", "AggregateConceptTransformerMultiplePrimaryAuthorities").
			WithField("primary_authorities", fmt.Sprintf("Smartlogic=%v, ManagedLocation=%v", slRecords, mlRecords)).
			Error("Error grouping concordance records")
		return nil, "", err
	}
	if primaryAuthority == "" {
		err = fmt.Errorf("no primary authority")
		logger.WithError(err).
			WithField("alert_tag", "AggregateConceptTransformerNoPrimaryAuthority").
			Error("Error grouping concordance records")
		return nil, "", err
	}
	return bucketedConcordances, primaryAuthority, nil
}

func (s *AggregateService) GetConcordedConcept(UUID string) (ConcordedConcept, string, error) {
	concordedConcept := ConcordedConcept{}
	concordedRecords, err := s.concordances.GetConcordance(UUID)
	if err != nil {
		return ConcordedConcept{}, "", err
	}
	logger.WithField("UUID", UUID).Debugf("Returned concordance record: %v", concordedRecords)

	bucketedConcordances, primaryAuthority, err := bucketConcordances(concordedRecords)
	if err != nil {
		return ConcordedConcept{}, "", err
	}

	// Get all concepts from S3
	for authority, concordanceRecords := range bucketedConcordances {
		if authority == primaryAuthority {
			continue
		}
		for _, conc := range concordanceRecords {
			found, sourceConcept, _, err := s.s3.GetConceptAndTransactionId(conc.UUID)
			if err != nil {
				return ConcordedConcept{}, "", err
			}

			if !found {
				//we should let the concorded concept to be written as a "Thing"
				logger.WithField("UUID", UUID).Warn(fmt.Sprintf("Source concept %s not found in S3", conc))
				sourceConcept.Authority = authority
				sourceConcept.AuthValue = conc.AuthorityValue
				sourceConcept.UUID = conc.UUID
				sourceConcept.Type = "Thing"
			}

			concordedConcept = mergeCanonicalInformation(concordedConcept, sourceConcept)
		}
	}

	canonicalConcept := bucketedConcordances[primaryAuthority][0]

	found, primaryConcept, transactionID, err := s.s3.GetConceptAndTransactionId(canonicalConcept.UUID)
	if err != nil {
		return ConcordedConcept{}, "", err
	} else if !found {
		err := fmt.Errorf("Canonical concept %s not found in S3", canonicalConcept.UUID)
		logger.WithField("UUID", UUID).Error(err.Error())
		return ConcordedConcept{}, "", err
	}

	concordedConcept = mergeCanonicalInformation(concordedConcept, primaryConcept)
	concordedConcept.Aliases = deduplicateAndSkipEmptyAliases(concordedConcept.Aliases)

	return concordedConcept, transactionID, nil
}

func (s *AggregateService) Healthchecks() []fthealth.Check {
	return []fthealth.Check{
		s.s3.Healthcheck(),
		s.conceptUpdatesSqs.Healthcheck(),
		s.RWElasticsearchHealthCheck(),
		s.RWNeo4JHealthCheck(),
		s.VarnishPurgerHealthCheck(),
		s.concordances.Healthcheck(),
		s.kinesis.Healthcheck(),
	}
}

func deduplicateAndSkipEmptyAliases(aliases []string) []string {
	aMap := map[string]bool{}
	var outAliases []string
	for _, v := range aliases {
		if v == "" {
			continue
		}
		aMap[v] = true
	}
	for a := range aMap {
		outAliases = append(outAliases, a)
	}
	return outAliases
}

func getMoreSpecificType(existingType string, newType string) string {
	if existingType == "PublicCompany" && (newType == "Organisation" || newType == "Company") {
		return existingType
	}
	return newType
}

func mergeCanonicalInformation(c ConcordedConcept, s s3.Concept) ConcordedConcept {
	c.PrefUUID = s.UUID
	c.PrefLabel = s.PrefLabel
	c.Type = getMoreSpecificType(c.Type, s.Type)
	c.Aliases = append(c.Aliases, s.Aliases...)
	c.Aliases = append(c.Aliases, s.PrefLabel)
	if s.Strapline != "" {
		c.Strapline = s.Strapline
	}
	if s.DescriptionXML != "" {
		c.DescriptionXML = s.DescriptionXML
	}
	if s.ImageURL != "" {
		c.ImageURL = s.ImageURL
	}
	if s.EmailAddress != "" {
		c.EmailAddress = s.EmailAddress
	}
	if s.FacebookPage != "" {
		c.FacebookPage = s.FacebookPage
	}
	if s.TwitterHandle != "" {
		c.TwitterHandle = s.TwitterHandle
	}
	if s.ScopeNote != "" {
		c.ScopeNote = s.ScopeNote
	}
	if s.ShortLabel != "" {
		c.ShortLabel = s.ShortLabel
	}
	if len(s.ParentUUIDs) > 0 {
		c.ParentUUIDs = s.ParentUUIDs
	}
	if len(s.BroaderUUIDs) > 0 {
		c.BroaderUUIDs = s.BroaderUUIDs
	}
	if len(s.RelatedUUIDs) > 0 {
		c.RelatedUUIDs = s.RelatedUUIDs
	}
	c.SourceRepresentations = append(c.SourceRepresentations, s)
	if s.ProperName != "" {
		c.ProperName = s.ProperName
	}
	if s.ShortName != "" {
		c.ShortName = s.ShortName
	}
	if len(s.TradeNames) > 0 {
		c.TradeNames = s.TradeNames
	}
	if len(s.FormerNames) > 0 {
		c.FormerNames = s.FormerNames
	}
	if s.CountryCode != "" {
		c.CountryCode = s.CountryCode
	}
	if s.CountryOfIncorporation != "" {
		c.CountryOfIncorporation = s.CountryOfIncorporation
	}
	if s.PostalCode != "" {
		c.PostalCode = s.PostalCode
	}
	if s.YearFounded > 0 {
		c.YearFounded = s.YearFounded
	}
	if s.LeiCode != "" {
		c.LeiCode = s.LeiCode
	}
	if s.IsAuthor {
		c.IsAuthor = s.IsAuthor
	}
	if s.BirthYear > 0 {
		c.BirthYear = s.BirthYear
	}
	if s.Salutation != "" {
		c.Salutation = s.Salutation
	}

	for _, mr := range s.MembershipRoles {
		c.MembershipRoles = append(c.MembershipRoles, MembershipRole{
			RoleUUID:        mr.RoleUUID,
			InceptionDate:   mr.InceptionDate,
			TerminationDate: mr.TerminationDate,
		})
	}
	if s.OrganisationUUID != "" {
		c.OrganisationUUID = s.OrganisationUUID
	}
	if s.PersonUUID != "" {
		c.PersonUUID = s.PersonUUID
	}
	if s.InceptionDate != "" {
		c.InceptionDate = s.InceptionDate
	}
	if s.TerminationDate != "" {
		c.TerminationDate = s.TerminationDate
	}
	if s.FigiCode != "" {
		c.FigiCode = s.FigiCode
	}
	if s.IssuedBy != "" {
		c.IssuedBy = s.IssuedBy
	}
	c.IsDeprecated = s.IsDeprecated
	return c
}

func sendToPurger(client httpClient, baseUrl string, conceptUUIDs []string, conceptType string, conceptTypesWithPublicEndpoints []string, tid string) error {

	req, err := http.NewRequest("POST", strings.TrimRight(baseUrl, "/")+"/purge", nil)
	if err != nil {
		return err
	}

	queryParams := req.URL.Query()
	for _, cUUID := range conceptUUIDs {
		queryParams.Add("target", thingsAPIEndpoint+"/"+cUUID)
		queryParams.Add("target", conceptsAPIEnpoint+"/"+cUUID)
	}

	if contains(conceptType, conceptTypesWithPublicEndpoints) {
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

func contains(element string, types []string) bool {
	for _, t := range types {
		if element == t {
			return true
		}
	}
	return false
}

func sendToWriter(client httpClient, baseUrl string, urlParam string, conceptUUID string, concept ConcordedConcept, tid string) (sqs.ConceptChanges, error) {
	updatedConcepts := sqs.ConceptChanges{}
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
	resp, err := client.Do(request)
	if err != nil {
		logger.WithError(err).WithTransactionID(tid).WithUUID(conceptUUID).Errorf("Request to %s returned status: %d", reqUrl, resp.StatusCode)
	}

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
	}
	if resp.StatusCode != 200 {
		err := errors.New("Request to " + reqUrl + " returned status: " + strconv.Itoa(resp.StatusCode) + "; skipping " + conceptUUID)
		logger.WithTransactionID(tid).WithUUID(conceptUUID).Errorf("Request to %s returned status: %d", reqUrl, resp.StatusCode)
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
