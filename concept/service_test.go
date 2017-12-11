package concept

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"sort"

	"github.com/Financial-Times/aggregate-concept-transformer/dynamodb"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/stretchr/testify/assert"
)

const (
	payload = `{
    			"UpdatedIds": [
        			"28090964-9997-4bc2-9638-7a11135aaff9",
        			"34a571fb-d779-4610-a7ba-2e127676db4d"
    			]
		 }`
	emptyPayload = `{
    			"UpdatedIds": [

    			]
		 }`
	esUrl    = "localhost:8080/__concept-rw-elasticsearch"
	neo4jUrl = "localhost:8080/__concepts-rw-neo4j"
)

type mockSQSClient struct {
	queue map[string]string
	s     sync.RWMutex
}

func (c *mockSQSClient) ListenAndServeQueue() []sqs.Notification {
	c.s.Lock()
	defer c.s.Unlock()
	q := c.queue
	notifications := []sqs.Notification{}
	for msgTag, UUID := range q {
		notifications = append(notifications, sqs.Notification{
			UUID:          UUID,
			ReceiptHandle: &msgTag,
		})
	}
	return notifications
}

func (c *mockSQSClient) RemoveMessageFromQueue(receiptHandle *string) error {
	c.s.Lock()
	defer c.s.Unlock()
	if _, ok := c.queue[*receiptHandle]; ok {
		delete(c.queue, *receiptHandle)
		return nil
	}
	return errors.New("Receipt handle not present on queue")
}

func (c *mockSQSClient) Queue() map[string]string {
	c.s.RLock()
	defer c.s.RUnlock()
	return c.queue
}

func (c *mockSQSClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}

type mockDynamoDBClient struct {
	concordances map[string][]string
	err          error
}

func (d *mockDynamoDBClient) GetConcordance(uuid string) (dynamodb.ConceptConcordance, error) {
	if corcordedIDs, ok := d.concordances[uuid]; ok {
		return dynamodb.ConceptConcordance{
			UUID:         uuid,
			ConcordedIds: corcordedIDs,
		}, d.err
	}
	return dynamodb.ConceptConcordance{
		UUID:         uuid,
		ConcordedIds: []string{},
	}, d.err
}
func (d *mockDynamoDBClient) Healthcheck() fthealth.Check {
	return fthealth.Check{}
}

type mockKinesisStreamClient struct {
	err error
}

func (k *mockKinesisStreamClient) AddRecordToStream(concept []byte, conceptType string) error {
	if k.err != nil {
		return k.err
	}
	return nil
}
func (d *mockKinesisStreamClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}

type mockS3Client struct {
	concepts map[string]struct {
		transactionID string
		concept       s3.Concept
	}
	err error
}

func (s *mockS3Client) GetConceptAndTransactionId(UUID string) (bool, s3.Concept, string, error) {
	if c, ok := s.concepts[UUID]; ok {
		return true, c.concept, c.transactionID, s.err
	}
	return false, s3.Concept{}, "", s.err
}
func (s *mockS3Client) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}

type mockHTTPClient struct {
	resp       string
	statusCode int
	err        error
}

func init() {
	logger.InitLogger("test-aggregate-concept-transformer", "debug")
}

func (c mockHTTPClient) Do(req *http.Request) (resp *http.Response, err error) {
	cb := ioutil.NopCloser(bytes.NewReader([]byte(c.resp)))
	return &http.Response{Body: cb, StatusCode: c.statusCode}, c.err
}

func TestNewService(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	assert.Equal(t, 5, len(svc.Healthchecks()))
}

func TestAggregateService_ListenForNotifications(t *testing.T) {
	svc, _, mockSqsClient, _, _ := setupTestService(200, payload)
	go svc.ListenForNotifications()
	time.Sleep(1 * time.Second)
	assert.Equal(t, 0, len(mockSqsClient.Queue()))
}

func TestAggregateService_ListenForNotifications_CannotProcessConceptNotInS3(t *testing.T) {
	svc, _, mockSqsClient, _, _ := setupTestService(200, payload)
	var receiptHandle string = "1"
	var nonExistingConcept string = "99247059-04ec-3abb-8693-a0b8951fdcab"
	mockSqsClient.queue[receiptHandle] = nonExistingConcept
	var expectedMap = make(map[string]string)
	expectedMap[receiptHandle] = nonExistingConcept
	go svc.ListenForNotifications()
	time.Sleep(50 * time.Microsecond)
	assert.Equal(t, expectedMap, mockSqsClient.queue)
	assert.Equal(t, 1, len(mockSqsClient.Queue()))
	err := mockSqsClient.RemoveMessageFromQueue(&receiptHandle)
	assert.NoError(t, err)
}

func TestAggregateService_ListenForNotifications_CannotProcessRemoveMessageNotPresentOnQueue(t *testing.T) {
	svc, _, mockSqsClient, _, _ := setupTestService(200, payload)
	var receiptHandle string = "2"
	go svc.ListenForNotifications()
	err := mockSqsClient.RemoveMessageFromQueue(&receiptHandle)
	assert.Error(t, err)
	assert.Equal(t, "Receipt handle not present on queue", err.Error())
}

func TestAggregateService_GetConcordedConcept_NoConcordance(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)

	c, tid, err := svc.GetConcordedConcept("99247059-04ec-3abb-8693-a0b8951fdcab")
	assert.NoError(t, err)
	assert.Equal(t, "tid_123", tid)
	assert.Equal(t, "Test Concept", c.PrefLabel)
}

func TestAggregateService_GetConcordedConcept_TMEConcordance(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)

	expectedConcept := ConcordedConcept{
		PrefUUID:      "28090964-9997-4bc2-9638-7a11135aaff9",
		PrefLabel:     "Root Concept",
		Type:          "Person",
		Aliases:       []string{"TME Concept", "Root Concept"},
		EmailAddress:  "person123@ft.com",
		FacebookPage:  "facebook/smartlogicPerson",
		TwitterHandle: "@FtSmartlogicPerson",
		ScopeNote:     "This note is in scope",
		ShortLabel:    "Concept",
		SourceRepresentations: []s3.Concept{
			{
				UUID:      "34a571fb-d779-4610-a7ba-2e127676db4d",
				PrefLabel: "TME Concept",
				Authority: "TME",
				AuthValue: "TME-123",
				Type:      "Person",
			},
			{
				UUID:          "28090964-9997-4bc2-9638-7a11135aaff9",
				PrefLabel:     "Root Concept",
				Authority:     "Smartlogic",
				AuthValue:     "28090964-9997-4bc2-9638-7a11135aaff9",
				Type:          "Person",
				FacebookPage:  "facebook/smartlogicPerson",
				TwitterHandle: "@FtSmartlogicPerson",
				ScopeNote:     "This note is in scope",
				EmailAddress:  "person123@ft.com",
				ShortLabel:    "Concept",
			},
		},
	}

	c, tid, err := svc.GetConcordedConcept("28090964-9997-4bc2-9638-7a11135aaff9")
	sort.Strings(c.Aliases)
	sort.Strings(expectedConcept.Aliases)
	assert.NoError(t, err)
	assert.Equal(t, "tid_456", tid)
	assert.Equal(t, expectedConcept, c)
}

func TestAggregateService_ProcessMessage_Success(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.NoError(t, err)
}

func TestAggregateService_ProcessMessage_GenericDynamoError(t *testing.T) {
	svc, _, _, mockDynamoClient, _ := setupTestService(200, payload)
	mockDynamoClient.err = errors.New("Could not get concordance record from DynamoDB")
	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Could not get concordance record from DynamoDB", err.Error())
}

func TestAggregateService_ProcessMessage_GenericS3Error(t *testing.T) {
	svc, mockS3Client, _, _, _ := setupTestService(200, payload)
	mockS3Client.err = errors.New("Error retrieving concept from S3")
	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Error retrieving concept from S3", err.Error())
}

func TestAggregateService_ProcessMessage_GenericWriterError(t *testing.T) {
	svc, _, _, _, _ := setupTestService(503, payload)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Request to localhost:8080/__concepts-rw-neo4j/people/28090964-9997-4bc2-9638-7a11135aaff9 returned status: 503; skipping 28090964-9997-4bc2-9638-7a11135aaff9", err.Error())
}

func TestAggregateService_ProcessMessage_GenericKinesisError(t *testing.T) {
	svc, _, _, _, mockKinesisClient := setupTestService(200, payload)
	mockKinesisClient.err = errors.New("Failed to add record to stream")

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Failed to add record to stream", err.Error())
}

func TestAggregateService_ProcessMessage_S3SourceNotFound(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	err := svc.ProcessMessage("c9d3a92a-da84-11e7-a121-0401beb96201")
	assert.Error(t, err)
	assert.Equal(t, "Source concept 3a3da730-0f4c-4a20-85a6-3ebd5776bd49 not found in S3", err.Error())
}

func TestAggregateService_ProcessMessage_S3CanonicalNotFound(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	err := svc.ProcessMessage("45f278ef-91b2-45f7-9545-fbc79c1b4004")
	assert.Error(t, err)
	assert.Equal(t, "Canonical concept 45f278ef-91b2-45f7-9545-fbc79c1b4004 not found in S3", err.Error())
}

func TestAggregateService_ProcessMessage_WriterReturnsNoUuids(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, emptyPayload)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.NoError(t, err)
}

func TestAggregateService_Healthchecks(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	healthchecks := svc.Healthchecks()

	for _, v := range healthchecks {
		s, e := v.Checker()
		assert.NoError(t, e)
		assert.Equal(t, "", s)
	}
}

func TestResolveConceptType(t *testing.T) {
	person := resolveConceptType("Person")
	assert.Equal(t, "people", person)

	specialReport := resolveConceptType("SpecialReport")
	assert.Equal(t, "special-reports", specialReport)

	alphavilleSeries := resolveConceptType("AlphavilleSeries")
	assert.Equal(t, "alphaville-series", alphavilleSeries)

	topic := resolveConceptType("Topic")
	assert.Equal(t, "topics", topic)
}

func setupTestService(httpError int, writerResponse string) (Service, *mockS3Client, *mockSQSClient, *mockDynamoDBClient, *mockKinesisStreamClient) {
	s3 := &mockS3Client{
		concepts: map[string]struct {
			transactionID string
			concept       s3.Concept
		}{
			"99247059-04ec-3abb-8693-a0b8951fdcab": {
				transactionID: "tid_123",
				concept: s3.Concept{
					UUID:      "99247059-04eFc-3abb-8693-a0b8951fdcab",
					PrefLabel: "Test Concept",
					Authority: "Smartlogic",
					AuthValue: "99247059-04ec-3abb-8693-a0b8951fdcab",
					Type:      "Person",
				},
			},
			"28090964-9997-4bc2-9638-7a11135aaff9": {
				transactionID: "tid_456",
				concept: s3.Concept{
					UUID:          "28090964-9997-4bc2-9638-7a11135aaff9",
					PrefLabel:     "Root Concept",
					Authority:     "Smartlogic",
					AuthValue:     "28090964-9997-4bc2-9638-7a11135aaff9",
					Type:          "Person",
					FacebookPage:  "facebook/smartlogicPerson",
					TwitterHandle: "@FtSmartlogicPerson",
					ScopeNote:     "This note is in scope",
					EmailAddress:  "person123@ft.com",
					ShortLabel:    "Concept",
				},
			},
			"34a571fb-d779-4610-a7ba-2e127676db4d": {
				transactionID: "tid_789",
				concept: s3.Concept{
					UUID:      "34a571fb-d779-4610-a7ba-2e127676db4d",
					PrefLabel: "TME Concept",
					Authority: "TME",
					AuthValue: "TME-123",
					Type:      "Person",
				},
			},
			"c9d3a92a-da84-11e7-a121-0401beb96201": {
				transactionID: "tid_629",
				concept: s3.Concept{
					UUID:      "c9d3a92a-da84-11e7-a121-0401beb96201",
					PrefLabel: "TME Concept",
					Authority: "TME",
					AuthValue: "TME-a2f",
					Type:      "Person",
				},
			},
		},
	}
	sqs := &mockSQSClient{
		queue: map[string]string{
			"1": "99247059-04ec-3abb-8693-a0b8951fdcab",
		},
	}
	dynamo := &mockDynamoDBClient{
		concordances: map[string][]string{
			"28090964-9997-4bc2-9638-7a11135aaff9": {"34a571fb-d779-4610-a7ba-2e127676db4d"},
			"c9d3a92a-da84-11e7-a121-0401beb96201": {"3a3da730-0f4c-4a20-85a6-3ebd5776bd49"},
			"4a4aaca0-b059-426c-bf4f-f00c6ef940ae": {"3a3da730-0f4c-4a20-85a6-3ebd5776bd49"},
		},
	}

	kinesis := &mockKinesisStreamClient{}

	return NewService(s3, sqs, dynamo, kinesis,
		neo4jUrl,
		esUrl,
		&mockHTTPClient{
			resp:       writerResponse,
			statusCode: httpError,
			err:        nil,
		},
	), s3, sqs, dynamo, kinesis
}
