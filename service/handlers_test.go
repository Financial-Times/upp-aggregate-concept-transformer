package service

import (
	"bytes"
	"encoding/json"
	"errors"
	ut "github.com/Financial-Times/aggregate-concept-transformer/util"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	ExpectedContentType = "application/json"
	UUID                = "61d707b5-6fab-3541-b017-49b72de80772"
	TID                 = "tid_newTid"
)

var invalidJson = `invalid`
var invalidPayload = `{"payload": "invalid"}`
var missingKeyPayload = `{"Records":[{"s3":{}}]}`
var vulcan = "http://localhost:8080/"
var expectedUuid = "e9aebb8d-a67f-355e-ad6a-32d6f6741200"

type mocks3Driver struct {
	uuid    string
	found   bool
	payload string
	resp    io.ReadCloser
	tid     string
	err     error
}

type mockSqsDriver struct {
}

func TestExtractConceptUuidFromSqsMessage_ReturnsUnmarshalErrorFromInvalidJson(t *testing.T) {
	_, err := extractConceptUuidFromSqsMessage(invalidJson)
	assert.Error(t, err, "Should have been able to parse message")
	assert.Contains(t, err.Error(), "Unmarshaling of concept json resulted in error")
}

func TestExtractConceptUuidFromSqsMessage_ReturnsCouldNotMapErrorFromInvalidPayload(t *testing.T) {
	_, err := extractConceptUuidFromSqsMessage(invalidPayload)
	assert.Error(t, err, "Should have been able to parse message")
	assert.Contains(t, err.Error(), "Could not map message ")
}

func TestExtractConceptUuidFromSqsMessage_ReturnsKeyIsNotUuidErrorFromInvalidPayload(t *testing.T) {
	_, err := extractConceptUuidFromSqsMessage(missingKeyPayload)
	assert.Error(t, err, "Should have been able to parse message")
	assert.Contains(t, err.Error(), "Could not extract concept uuid from message")
}

func TestExtractConceptUuidFromSqsMessage_ReturnsErrorFromInvalidUuidInMessage(t *testing.T) {
	file, err := ioutil.ReadFile("../util/sqsMessage/testInvalidUuidSqsMessage.json")
	assert.NoError(t, err, "Should have been able to read test file")
	_, err = extractConceptUuidFromSqsMessage(string(file))
	assert.Error(t, err, "Should have been able to parse message")
	assert.Equal(t, err.Error(), "Message key: invalidUuid, is not a valid uuid")
}

func TestExtractConceptUuidFromSqsMessage_ReturnsUuidFromValidMessage(t *testing.T) {
	file, err := ioutil.ReadFile("../util/sqsMessage/testValidSqsMessage.json")
	assert.NoError(t, err, "Should have been able to read test file")
	extractedUuid, err := extractConceptUuidFromSqsMessage(string(file))
	assert.NoError(t, err, "Should have been able to parse message")
	assert.Equal(t, expectedUuid, extractedUuid)
}

func TestExtractConceptUuidFromSqsMessage_ReturnsErrorFromMissingUuidInMessage(t *testing.T) {
	file, err := ioutil.ReadFile("../util/sqsMessage/testMissingUuidSqsMessage.json")
	assert.NoError(t, err, "Should have been able to read test file")
	_, err = extractConceptUuidFromSqsMessage(string(file))
	assert.Error(t, err, "Should have been able to parse message")
	assert.Contains(t, err.Error(), "Could not extract concept uuid from message:")
}

func TestResolveMessageType_ReturnCorrectMessageTypes(t *testing.T) {
	messageTypePerson := resolveConceptType("person")
	assert.Equal(t, "people", messageTypePerson)
	messageTypeAlphaSer := resolveConceptType("alphavilleseries")
	assert.Equal(t, "alphaville-series", messageTypeAlphaSer)
	messageTypeSpecRep := resolveConceptType("specialreports")
	assert.Equal(t, "special-reports", messageTypeSpecRep)
	messageTypeOther := resolveConceptType("topic")
	assert.Equal(t, "topics", messageTypeOther)
}

func TestMapJson_SuccessfullyMapFromOldWorldJsonToNew(t *testing.T) {
	conceptJson, err := ioutil.ReadFile("../util/conceptJson/oldWorldGenre.json")
	assert.NoError(t, err, "Should have been able to read test file")
	sourceConceptModel := ut.SourceConceptJson{}
	json.Unmarshal(conceptJson, &sourceConceptModel)
	concordedJson, err := mapJson(sourceConceptModel)
	assert.NoError(t, err, "All values should be present in json")
	assert.Equal(t, "61d707b5-6fab-3541-b017-49b72de80772", concordedJson.UUID)
	assert.Equal(t, "Analysis", concordedJson.PrefLabel)
	assert.Equal(t, "Genre", concordedJson.Type)
	assert.Equal(t, "TME", concordedJson.SourceRepresentations[0].Authority)
	assert.Equal(t, "MQ==-R2VucmVz", concordedJson.SourceRepresentations[0].AuthValue)
}

func TestMapJson_MappingInvalidJsonThrowsErrors(t *testing.T) {
	type testStruct struct {
		Json          string
		expectedError string
	}
	sourceConceptModel := ut.SourceConceptJson{}

	BlankJson := testStruct{Json: `{}`, expectedError: "uuid must not be blank"}
	OnlyUuid := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200"}`, expectedError: "prefLabel must not be blank"}
	OnlyUuidPrefLabel := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200", "prefLabel":"prefLabel"}`, expectedError: "type must not be blank"}
	NoIdentifier := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200", "prefLabel":"prefLabel", "type":"concept"}`, expectedError: "must have source identifier"}
	Collections := []testStruct{BlankJson, OnlyUuid, OnlyUuidPrefLabel, NoIdentifier}

	for _, c := range Collections {
		json.Unmarshal([]byte(c.Json), &sourceConceptModel)
		_, err := mapJson(sourceConceptModel)
		assert.Error(t, err, "All values should be present in json")
		assert.Contains(t, err.Error(), c.expectedError)
	}
}

func TestGetHandler_ResponseCodeAndErrorMessageWhenBadConnectionToS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false, err: errors.New("Cannot connect to s3")}
	mSqsDriver := mockSqsDriver{}
	h := NewHandler(&ms3d, &mSqsDriver, vulcan)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, "genre"))

	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Error retrieving concept.\"}", rec.Body.String())
}

func TestGetHandler_ResponseCodeAndErrorWhenConceptNotFoundInS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false}
	mSqsDriver := mockSqsDriver{}
	h := NewHandler(&ms3d, &mSqsDriver, vulcan)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, "genre"))

	assert.Equal(t, 404, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Concept not found.\"}", rec.Body.String())
}

func TestGetHandler_TransactionIdIsGeneratedIfBucketDoesNotHaveOne(t *testing.T) {
	r := mux.NewRouter()
	genre, err := ioutil.ReadFile("../util/conceptJson/oldWorldGenre.json")
	assert.NoError(t, err, "Error reading file ")
	ms3d := mocks3Driver{found: true, payload: string(genre)}
	mSqsDriver := mockSqsDriver{}
	h := NewHandler(&ms3d, &mSqsDriver, vulcan)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, string(genre)))

	assert.Equal(t, 200, rec.Code)
	assert.NotEqual(t, rec.HeaderMap["X-Request-Id"], []string{TID})
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
}

func TestGetHandler_InvalidConceptJsonThrowsError(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: true, payload: invalidJson}
	mSqsDriver := mockSqsDriver{}
	h := NewHandler(&ms3d, &mSqsDriver, vulcan)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, invalidPayload))

	assert.Equal(t, 422, rec.Code)
	assert.Equal(t, []string{"application/json"}, rec.HeaderMap["Content-Type"])
	assert.Equal(t, "{\"message\":\"Invalid json returned from s3\"}", rec.Body.String())
}

func TestGetHandler_IncompleteConceptJsonThrowsErrorWhenMapped(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: true, payload: `{}`}
	mSqsDriver := mockSqsDriver{}
	h := NewHandler(&ms3d, &mSqsDriver, vulcan)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, invalidPayload))

	assert.Equal(t, 422, rec.Code)
	assert.Equal(t, []string{"application/json"}, rec.HeaderMap["Content-Type"])
	assert.Equal(t, "{\"message\":\"Json from s3 cannot be concorded as it is missing key fields\"}", rec.Body.String())
}

func TestGetHandler_ValidConceptGetsReturned(t *testing.T) {
	r := mux.NewRouter()
	genre, err := ioutil.ReadFile("../util/conceptJson/oldWorldGenre.json")
	assert.NoError(t, err, "Error reading file ")
	ms3d := mocks3Driver{found: true, payload: string(genre), tid: TID}
	mSqsDriver := mockSqsDriver{}
	h := NewHandler(&ms3d, &mSqsDriver, vulcan)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, string(genre)))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, rec.HeaderMap["X-Request-Id"], []string{TID})
}

func (md *mocks3Driver) GetConceptAndTransactionId(UUID string) (bool, io.ReadCloser, string, error) {
	md.uuid = UUID
	var body io.ReadCloser
	var tid string
	if md.payload != "" {
		body = ioutil.NopCloser(strings.NewReader(md.payload))
	}

	if md.resp != nil {
		body = md.resp
	}
	if md.tid != "" {
		tid = md.tid
	}
	return md.found, body, tid, md.err
}

func (md *mocks3Driver) HealthCheck() (string, error) {
	return "OK", nil
}

func (md *mockSqsDriver) HealthCheck() (string, error) {
	return "OK", nil
}

func (md *mockSqsDriver) ListenAndServeQueue() []*awsSqs.Message {
	return []*awsSqs.Message{}
}

func (md *mockSqsDriver) RemoveMessageFromQueue(receiptHandle *string) error {
	return nil
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
