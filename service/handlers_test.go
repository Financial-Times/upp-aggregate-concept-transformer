package service

import (
	"bytes"
	"github.com/Financial-Times/aggregate-concept-transformer/kafka"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"net/http/httptest"
	"github.com/stretchr/testify/assert"
	"errors"
)

const (
	ExpectedContentType = "application/json"
	TOPIC               = "Concept"
	UUID		    = "61d707b5-6fab-3541-b017-49b72de80772"
)

var testGenrePayload = `{
  "uuid": "61d707b5-6fab-3541-b017-49b72de80772",
  "alternativeIdentifiers": {
    "TME": [
      "MQ==-R2VucmVz"
    ],
    "uuids": [
      "61d707b5-6fab-3541-b017-49b72de80772"
    ]
  },
  "prefLabel": "Analysis",
  "type": "Genre"
}`

var invalidPayload = `{Payload}`

type mocks3Driver struct {
	uuid    string
	found   bool
	payload string
	resp    io.ReadCloser
	err     error
}

type mockSyncProducer struct {
	err error
}

func TestGetHandler_ResponseCodeAndErrorMessageWhenBadConnectionToS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false, err: errors.New("")}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)


	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/" + UUID, "genre"))

	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Error retrieving concept.\"}", rec.Body.String())
}

func TestGetHandler_ResponseCodeAndErrorWhenConceptNotFoundInS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/" + UUID, "genre"))

	assert.Equal(t, 404, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Concept not found.\"}", rec.Body.String())
}

func TestGetHandler_ValidConceptGetsReturned(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: true, payload: "genre"}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/" + UUID, testGenrePayload))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
}

func TestPostHandler_ResponseCodeAndErrorMessageWhenBadConnectionToS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false, err: errors.New("")}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("POST", "/concept/" + UUID, testGenrePayload))

	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Error retrieving concept.\"}", rec.Body.String())
}

func TestPostHandler_ResponseCodeAndErrorWhenConceptNotFoundInS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("POST", "/concept/" + UUID, testGenrePayload))

	assert.Equal(t, 404, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Concept not found.\"}", rec.Body.String())
}

func TestPostHandler_InvalidJsonThrowsError(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: true, payload: invalidPayload}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("POST", "/concept/" + UUID, invalidPayload))

	assert.Equal(t, 422, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Retrived concept is invalid json.\"}", rec.Body.String())
}

func TestPostHandler_SendMessageToKafkaThrowsError(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: true, payload: testGenrePayload}
	mkc := kafka.Client{Producer: &mockSyncProducer{err: errors.New("Failed to produce message")}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	_, _, err := mkc.Producer.SendMessage(nil)

	assert.Error(t,err)
}

func TestPostHandler_ValidConceptGetsWrittenToS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: true, payload: testGenrePayload}
	mkc := kafka.Client{Producer: &mockSyncProducer{}, Topic: TOPIC}
	h := NewHandler(&ms3d, mkc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("POST", "/concept/" + UUID, testGenrePayload))
	_, _, err := mkc.Producer.SendMessage(nil)

	assert.NoError(t,err)
	assert.Equal(t, 202, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
}

func (md *mocks3Driver) GetConcept(UUID string) (bool, io.ReadCloser, error) {
	md.uuid = UUID
	var body io.ReadCloser
	if md.payload != "" {
		body = ioutil.NopCloser(strings.NewReader(md.payload))
	}

	if md.resp != nil {
		body = md.resp
	}
	return md.found, body, md.err
}

func (md *mocks3Driver) HealthCheck() (string, error) {
	return "OK", nil
}

func (msp *mockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	var err error
	if msp.err != nil {
		err = msp.err
	}
	return 0, 0, err
}

func (msp *mockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	msp.err = nil
	return msp.err
}

func (msp *mockSyncProducer) Close() error {
	msp.err = nil
	return msp.err
}

func newRequest(method, url string, body string) *http.Request {
	var payload io.Reader
	if body != "" {
		payload = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, url, payload)
	req.Header = map[string][]string{
		"Content-Type": {ExpectedContentType},
	}
	if err != nil {
		panic(err)
	}
	return req
}
