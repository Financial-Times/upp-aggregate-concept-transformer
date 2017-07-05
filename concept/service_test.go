package concept

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Financial-Times/aggregate-concept-transformer/dynamodb"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/stretchr/testify/assert"
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
	return errors.New("Not found")
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

func (c mockHTTPClient) Do(req *http.Request) (resp *http.Response, err error) {
	cb := ioutil.NopCloser(bytes.NewReader([]byte(c.resp)))
	return &http.Response{Body: cb, StatusCode: c.statusCode}, c.err
}

func TestNewService(t *testing.T) {
	svc, _, _, _ := setupTestService(200)
	assert.Equal(t, 4, len(svc.Healthchecks()))
}

func TestAggregateService_ListenForNotifications(t *testing.T) {
	svc, _, sqs, _ := setupTestService(200)
	go svc.ListenForNotifications()
	time.Sleep(1 * time.Second)
	assert.Equal(t, 0, len(sqs.Queue()))
}

func TestAggregateService_GetConcordedConcept_NoConcordance(t *testing.T) {
	svc, _, _, _ := setupTestService(200)

	c, tid, err := svc.GetConcordedConcept("99247059-04ec-3abb-8693-a0b8951fdcab")
	assert.NoError(t, err)
	assert.Equal(t, "tid_123", tid)
	assert.Equal(t, "Test Concept", c.PrefLabel)
}

func TestAggregateService_GetConcordedConcept_TMEConcordance(t *testing.T) {
	svc, _, _, _ := setupTestService(200)

	c, tid, err := svc.GetConcordedConcept("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.NoError(t, err)
	assert.Equal(t, "tid_456", tid)
	assert.Equal(t, ConcordedConcept{
		PrefUUID:  "28090964-9997-4bc2-9638-7a11135aaff9",
		PrefLabel: "Root Concept",
		Type:      "Person",
		Aliases:   []string{"TME Concept", "Root Concept"},
		SourceRepresentations: []s3.Concept{
			{
				UUID:      "34a571fb-d779-4610-a7ba-2e127676db4d",
				PrefLabel: "TME Concept",
				Authority: "TME",
				AuthValue: "TME-123",
				Type:      "Person",
			},
			{
				UUID:      "28090964-9997-4bc2-9638-7a11135aaff9",
				PrefLabel: "Root Concept",
				Authority: "Smartlogic",
				AuthValue: "28090964-9997-4bc2-9638-7a11135aaff9",
				Type:      "Person",
			},
		},
	}, c)
}

func TestAggregateService_ProcessMessage_Success(t *testing.T) {
	svc, _, _, _ := setupTestService(200)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.NoError(t, err)
}

func TestAggregateService_ProcessMessage_NeoFail(t *testing.T) {
	svc, _, _, _ := setupTestService(503)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Request to neoAddress/people/28090964-9997-4bc2-9638-7a11135aaff9 returned status: 503; skipping 28090964-9997-4bc2-9638-7a11135aaff9", err.Error())
}

func TestAggregateService_ProcessMessage_NotFound(t *testing.T) {
	svc, _, _, _ := setupTestService(200)

	err := svc.ProcessMessage("090905b8-e0d5-41e6-b9e4-21171ab73dc1")
	assert.Error(t, err)
	assert.Equal(t, "SL Concept not found: 090905b8-e0d5-41e6-b9e4-21171ab73dc1", err.Error())
}

func TestAggregateService_Healthchecks(t *testing.T) {
	svc, _, _, _ := setupTestService(200)
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

func setupTestService(httpError int) (Service, *mockS3Client, *mockSQSClient, *mockDynamoDBClient) {
	s3 := &mockS3Client{
		concepts: map[string]struct {
			transactionID string
			concept       s3.Concept
		}{
			"99247059-04ec-3abb-8693-a0b8951fdcab": {
				transactionID: "tid_123",
				concept: s3.Concept{
					UUID:      "99247059-04ec-3abb-8693-a0b8951fdcab",
					PrefLabel: "Test Concept",
					Authority: "Smartlogic",
					AuthValue: "99247059-04ec-3abb-8693-a0b8951fdcab",
					Type:      "Person",
				},
			},
			"28090964-9997-4bc2-9638-7a11135aaff9": {
				transactionID: "tid_456",
				concept: s3.Concept{
					UUID:      "28090964-9997-4bc2-9638-7a11135aaff9",
					PrefLabel: "Root Concept",
					Authority: "Smartlogic",
					AuthValue: "28090964-9997-4bc2-9638-7a11135aaff9",
					Type:      "Person",
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
		},
	}

	return NewService(s3, sqs, dynamo,
		"neoAddress",
		"esAddress",
		&mockHTTPClient{
			resp:       "",
			statusCode: httpError,
			err:        nil,
		},
	), s3, sqs, dynamo
}
