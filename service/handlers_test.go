package service

import (
	"bytes"
	"encoding/json"
	"errors"
	ut "github.com/Financial-Times/aggregate-concept-transformer/util"
	"github.com/aws/aws-sdk-go/aws"
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
	UUID                = "e9aebb8d-a67f-355e-ad6a-32d6f6741200"
	TID                 = "tid_newTid"
	VULCAN              = "http://localhost:8080/"
)

var invalidJson = `{"payload":}`
var invalidPayload = `{"payload": "invalid"}`
var missingKeyPayload = `{"Records":[{"s3":{}}]}`

type mocks3Driver struct {
	uuid    string
	found   bool
	payload string
	resp    io.ReadCloser
	tid     string
	err     error
}

type mockSqsDriver struct {
	err error
}

type mockHttpClient struct {
	resp       string
	statusCode int
	err        error
}

func TestAdminHandler_PingAndBuildInfo(t *testing.T) {
	r := mux.NewRouter()
	mhc := mockHttpClient{}
	ms3d := mocks3Driver{}
	msQsD := mockSqsDriver{}
	h := NewHandler(&ms3d, &msQsD, VULCAN, &mhc)
	h.RegisterAdminHandlers(r)
	rec := httptest.NewRecorder()

	type testStruct struct {
		endpoint           string
		expectedStatusCode int
		expectedBody       string
		expectedError      string
	}

	pingChecker := testStruct{endpoint: "/__ping", expectedStatusCode: 200, expectedBody: "pong"}
	buildInfoChecker := testStruct{endpoint: "/__build-info", expectedStatusCode: 200, expectedBody: "Version  is not a semantic version"}

	Collections := []testStruct{pingChecker, buildInfoChecker}

	for _, col := range Collections {
		http.DefaultServeMux.ServeHTTP(rec, newRequest("GET", col.endpoint, ""))
		assert.Equal(t, col.expectedStatusCode, rec.Code)
		assert.Contains(t, rec.Body.String(), col.expectedBody)
	}
}

func TestProcessMessages_ErrorHandling(t *testing.T) {
	r := mux.NewRouter()
	genre := getFileAsBytesArray(t, "../util/conceptJson/genre.json")
	validMessage := awsSqs.Message{Body: aws.String(string(getFileAsBytesArray(t, "../util/sqsMessage/sqsMessage_valid.json")))}
	invalidMessage := awsSqs.Message{Body: aws.String("")}
	invalidUuid := awsSqs.Message{Body: aws.String(string(getFileAsBytesArray(t, "../util/sqsMessage/sqsMessage_InvalidUuid.json")))}

	type testStruct struct {
		message       awsSqs.Message
		ms3d          mocks3Driver
		msQsD         mockSqsDriver
		mhc           mockHttpClient
		expectedError string
	}

	jsonUnmarshalMessageError := testStruct{message: invalidMessage, expectedError: "unexpected end of JSON input", ms3d: mocks3Driver{}, mhc: mockHttpClient{}}
	invalidUuidError := testStruct{message: invalidUuid, expectedError: "Message key: invalidUuid, is not a valid uuid", ms3d: mocks3Driver{}, mhc: mockHttpClient{}}
	s3NotAvailable := testStruct{message: validMessage, expectedError: "Error retrieving concept", ms3d: mocks3Driver{found: false, err: errors.New("Cannot connect to s3")}, mhc: mockHttpClient{}}
	s3ConceptNotFound := testStruct{message: validMessage, expectedError: "Concept not found", ms3d: mocks3Driver{found: false}, mhc: mockHttpClient{}}
	jsonUnmarshalConceptError := testStruct{message: validMessage, expectedError: "cannot unmarshal string", ms3d: mocks3Driver{found: true, payload: "\"payload\""}, mhc: mockHttpClient{}}
	invalidConceptJsonError := testStruct{message: validMessage, expectedError: "Invalid Concept", ms3d: mocks3Driver{found: true, payload: invalidPayload}, mhc: mockHttpClient{}}
	conceptRw404 := testStruct{message: validMessage, expectedError: "Request to http://localhost:8080/__concepts-rw-neo4j/genres/e9aebb8d-a67f-355e-ad6a-32d6f6741200 returned status: 404", ms3d: mocks3Driver{found: true, payload: string(genre)}, mhc: mockHttpClient{statusCode: 404}}
	conceptRw503 := testStruct{message: validMessage, expectedError: "Request to http://localhost:8080/__concepts-rw-neo4j/genres/e9aebb8d-a67f-355e-ad6a-32d6f6741200 returned status: 503", ms3d: mocks3Driver{found: true, payload: string(genre)}, mhc: mockHttpClient{statusCode: 503, err: errors.New("Unavailable")}}
	deleteMessageError := testStruct{message: validMessage, expectedError: "Receipt handle does not exist", ms3d: mocks3Driver{found: true, payload: string(genre)}, mhc: mockHttpClient{statusCode: 200}, msQsD: mockSqsDriver{err: errors.New("Receipt handle does not exist")}}

	Collections := []testStruct{jsonUnmarshalMessageError, invalidUuidError, s3NotAvailable, s3ConceptNotFound, jsonUnmarshalConceptError, invalidConceptJsonError, conceptRw404, conceptRw503, deleteMessageError}

	for _, collection := range Collections {
		h := NewHandler(&collection.ms3d, &collection.msQsD, VULCAN, &collection.mhc)
		h.RegisterHandlers(r)

		err := h.processMessage(&collection.message)
		assert.Error(t, err, "Should be an error")
		assert.Contains(t, err.Error(), collection.expectedError)
	}
}

func TestProcessMessages_ProcessesValidMessageWithNoErrors(t *testing.T) {
	r := mux.NewRouter()
	genre := getFileAsBytesArray(t, "../util/conceptJson/genre.json")
	validMessage := awsSqs.Message{Body: aws.String(string(getFileAsBytesArray(t, "../util/sqsMessage/sqsMessage_valid.json")))}

	type testStruct struct {
		message       awsSqs.Message
		ms3d          mocks3Driver
		msQsD         mockSqsDriver
		mhc           mockHttpClient
		expectedError string
	}

	messageProcessedWithNoErrors := testStruct{message: validMessage, ms3d: mocks3Driver{found: true, payload: string(genre)}, mhc: mockHttpClient{statusCode: 200}}

	h := NewHandler(&messageProcessedWithNoErrors.ms3d, &messageProcessedWithNoErrors.msQsD, VULCAN, &messageProcessedWithNoErrors.mhc)
	h.RegisterHandlers(r)

	err := h.processMessage(&messageProcessedWithNoErrors.message)
	assert.NoError(t, err, "Should not be any errors")

}

func TestExtractConceptUuidFromSqsMessage_ErrorHandling(t *testing.T) {
	type testStruct struct {
		Json          string
		expectedError string
	}

	invalidUuidFile := getFileAsBytesArray(t, "../util/sqsMessage/sqsRecord_InvalidUuid.json")
	Test1 := testStruct{Json: string(invalidUuidFile), expectedError: "Message key: invalidUuid, is not a valid uuid"}
	Test2 := testStruct{Json: invalidJson, expectedError: "Unmarshaling of concept json resulted in error"}
	Test3 := testStruct{Json: invalidPayload, expectedError: "Could not map message "}
	Test4 := testStruct{Json: missingKeyPayload, expectedError: "Could not extract concept uuid from message"}
	Collections := []testStruct{Test1, Test2, Test3, Test4}

	for _, c := range Collections {
		_, err := extractConceptUuidFromSqsMessage(c.Json)
		assert.Error(t, err, c.Json+" should throw error:"+c.expectedError)
		assert.Contains(t, err.Error(), c.expectedError)
	}
}

func TestExtractConceptUuidFromSqsMessage_ReturnsUuidFromValidMessage(t *testing.T) {
	file := getFileAsBytesArray(t, "../util/sqsMessage/sqsRecord_valid.json")
	extractedUuid, err := extractConceptUuidFromSqsMessage(string(file))
	assert.NoError(t, err, "Should have been able to parse message")
	assert.Equal(t, UUID, extractedUuid)
}

func TestResolveMessageType_ReturnCorrectMessageTypes(t *testing.T) {
	messageTypePerson := resolveConceptType("person")
	assert.Equal(t, "people", messageTypePerson)
	messageTypeAlphaSer := resolveConceptType("alphavilleseries")
	assert.Equal(t, "alphaville-series", messageTypeAlphaSer)
	messageTypeSpecRep := resolveConceptType("specialreport")
	assert.Equal(t, "special-reports", messageTypeSpecRep)
	messageTypeOther := resolveConceptType("topic")
	assert.Equal(t, "topics", messageTypeOther)
}

func TestMapJson_MappingInvalidJsonThrowsErrors(t *testing.T) {
	type testStruct struct {
		Json          string
		expectedError string
	}
	sourceConceptModel := ut.SourceRepresentation{}

	BlankJson := testStruct{Json: `{}`, expectedError: "uuid field must not be blank"}
	OnlyUuid := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200"}`, expectedError: "prefLabel field must not be blank"}
	OnlyUuidPrefLabel := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200", "prefLabel":"prefLabel"}`, expectedError: "type field must not be blank"}
	NoIdentifierAuthority := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200", "prefLabel":"prefLabel", "type":"concept"}`, expectedError: "identifier authority field must not be blank"}
	NoIdentifierValue := testStruct{Json: `{"uuid":"e9aebb8d-a67f-355e-ad6a-32d6f6741200", "prefLabel":"prefLabel", "type":"concept", "authority":"TME"}`, expectedError: "identifier value field must not be blank"}
	Collections := []testStruct{BlankJson, OnlyUuid, OnlyUuidPrefLabel, NoIdentifierAuthority, NoIdentifierValue}

	for _, c := range Collections {
		json.Unmarshal([]byte(c.Json), &sourceConceptModel)
		_, err := mapJson(sourceConceptModel, UUID)
		assert.Error(t, err, "All values should be present in json")
		assert.Contains(t, err.Error(), c.expectedError)
	}
}

func TestMapJson_SuccessfullyMapFromOldWorldJsonToNew(t *testing.T) {
	conceptJson := getFileAsBytesArray(t, "../util/conceptJson/person.json")
	sourceConceptModel := ut.SourceRepresentation{}
	json.Unmarshal(conceptJson, &sourceConceptModel)
	concordedJson, err := mapJson(sourceConceptModel, UUID)
	assert.NoError(t, err, "All values should be present in json")
	assert.Equal(t, "b8baa012-1fa7-4a01-a90b-ebf9852edac8", concordedJson.UUID)
	assert.Equal(t, "David Chapman", concordedJson.PrefLabel)
	assert.Equal(t, "Person", concordedJson.Type)
	assert.Equal(t, "TME", concordedJson.SourceRepresentations[0].Authority)
	assert.Equal(t, "TnN0Njc=-UE4=", concordedJson.SourceRepresentations[0].AuthValue)
	assert.Equal(t, "Dave Chapman", concordedJson.SourceRepresentations[0].Aliases[0])
}

func TestGetHandler_ResponseCodeAndErrorMessageWhenBadConnectionToS3(t *testing.T) {
	r := mux.NewRouter()
	ms3d := mocks3Driver{found: false, err: errors.New("Cannot connect to s3")}
	mSqsDriver := mockSqsDriver{}
	mhc := mockHttpClient{}
	h := NewHandler(&ms3d, &mSqsDriver, VULCAN, mhc)
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
	mhc := mockHttpClient{}
	h := NewHandler(&ms3d, &mSqsDriver, VULCAN, mhc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, "genre"))

	assert.Equal(t, 404, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, "{\"message\":\"Concept not found.\"}", rec.Body.String())
}

func TestGetHandler_TransactionIdIsGeneratedIfBucketDoesNotHaveOne(t *testing.T) {
	r := mux.NewRouter()
	genre := getFileAsBytesArray(t, "../util/conceptJson/genre.json")
	ms3d := mocks3Driver{found: true, payload: string(genre)}
	mSqsDriver := mockSqsDriver{}
	mhc := mockHttpClient{}
	h := NewHandler(&ms3d, &mSqsDriver, VULCAN, mhc)
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
	mhc := mockHttpClient{}
	h := NewHandler(&ms3d, &mSqsDriver, VULCAN, &mhc)
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
	mhc := mockHttpClient{}
	h := NewHandler(&ms3d, &mSqsDriver, VULCAN, &mhc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, invalidPayload))

	assert.Equal(t, 422, rec.Code)
	assert.Equal(t, []string{"application/json"}, rec.HeaderMap["Content-Type"])
	assert.Equal(t, "{\"message\":\"Json from s3 cannot be concorded as it is missing key fields\"}", rec.Body.String())
}

func TestGetHandler_ValidConceptGetsReturned(t *testing.T) {
	r := mux.NewRouter()
	genre := getFileAsBytesArray(t, "../util/conceptJson/genre.json")
	ms3d := mocks3Driver{found: true, payload: string(genre), tid: TID}
	mSqsDriver := mockSqsDriver{}
	mhc := mockHttpClient{}
	h := NewHandler(&ms3d, &mSqsDriver, VULCAN, &mhc)
	h.RegisterHandlers(r)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, newRequest("GET", "/concept/"+UUID, string(genre)))

	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, rec.HeaderMap["Content-Type"], []string{"application/json"})
	assert.Equal(t, rec.HeaderMap["X-Request-Id"], []string{TID})
}

func getFileAsBytesArray(t *testing.T, fileName string) []byte {
	fullMessage, err := ioutil.ReadFile(fileName)
	assert.NoError(t, err, "Error reading file ")
	return fullMessage
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

func (md *mocks3Driver) HealthCheck() error {
	return md.err
}

func (md *mockSqsDriver) HealthCheck() error {
	return md.err
}

func (md *mockSqsDriver) ListenAndServeQueue() []*awsSqs.Message {
	return []*awsSqs.Message{}
}

func (md *mockSqsDriver) RemoveMessageFromQueue(receiptHandle *string) error {
	return md.err
}

func (c mockHttpClient) Do(req *http.Request) (resp *http.Response, err error) {
	cb := ioutil.NopCloser(bytes.NewReader([]byte(c.resp)))
	return &http.Response{Body: cb, StatusCode: c.statusCode}, c.err
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
