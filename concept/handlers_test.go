package concept

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"sync"

	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

const ExpectedContentType = "application/json"

func TestHandlers(t *testing.T) {
	testCases := []struct {
		name          string
		method        string
		url           string
		requestBody   string
		resultCode    int
		resultBody    string
		err           error
		concepts      map[string]ConcordedConcept
		notifications []sqs.Notification
		healthchecks  []fthealth.Check
	}{
		{
			"Get Concept - Success",
			"GET",
			"/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097",
			"",
			200,
			"{\"prefUUID\":\"f7fd05ea-9999-47c0-9be9-c99dd84d0097\",\"prefLabel\":\"TestConcept\"}\n",
			nil,
			map[string]ConcordedConcept{
				"f7fd05ea-9999-47c0-9be9-c99dd84d0097": {
					PrefUUID:  "f7fd05ea-9999-47c0-9be9-c99dd84d0097",
					PrefLabel: "TestConcept",
				},
			},
			[]sqs.Notification{},
			nil,
		},
		{
			"Get Concept - Not Found",
			"GET",
			"/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097",
			"",
			404,
			"Could not load the full concept",
			nil,
			map[string]ConcordedConcept{},
			[]sqs.Notification{},
			nil,
		},
		{
			"Send Concept - Success",
			"POST",
			"/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097/send",
			"",
			200,
			"Concept f7fd05ea-9999-47c0-9be9-c99dd84d0097 sent successfully.",
			nil,
			map[string]ConcordedConcept{
				"f7fd05ea-9999-47c0-9be9-c99dd84d0097": {
					PrefUUID:  "f7fd05ea-9999-47c0-9be9-c99dd84d0097",
					PrefLabel: "TestConcept",
				},
			},
			[]sqs.Notification{},
			nil,
		},
		{
			"Send Concept - Failure",
			"POST",
			"/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097/send",
			"",
			500,
			"Could not process the concept",
			nil,
			map[string]ConcordedConcept{},
			[]sqs.Notification{},
			nil,
		},
		{
			"GTG - Success",
			"GET",
			"/__gtg",
			"",
			200,
			"OK",
			nil,
			map[string]ConcordedConcept{},
			[]sqs.Notification{},
			nil,
		},
		{
			"GTG - Failure",
			"GET",
			"/__gtg",
			"",
			503,
			"GTG fail error",
			nil,
			map[string]ConcordedConcept{},
			[]sqs.Notification{},
			[]fthealth.Check{
				{
					Checker: func() (string, error) {
						return "", errors.New("GTG fail error")
					},
				},
			},
		},
	}

	for _, d := range testCases {
		t.Run(d.name, func(t *testing.T) {
			mockService := NewMockService(d.concepts, d.notifications, d.healthchecks)
			handler := NewHandler(mockService)
			m := mux.NewRouter()
			handler.RegisterHandlers(m)
			handler.RegisterAdminHandlers(m, NewHealthService(mockService, "system-code", "app-name", "8080"))

			req, _ := http.NewRequest(d.method, d.url, bytes.NewBufferString(d.requestBody))
			rr := httptest.NewRecorder()
			m.ServeHTTP(rr, req)

			b, err := ioutil.ReadAll(rr.Body)
			assert.NoError(t, err)
			body := string(b)
			assert.Equal(t, d.resultCode, rr.Code, d.name)
			if d.resultBody != "IGNORE" {
				assert.Equal(t, d.resultBody, body, d.name)
			}
		})
	}
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

type MockService struct {
	notifications []sqs.Notification
	concepts      map[string]ConcordedConcept
	m             sync.RWMutex
	healthchecks  []fthealth.Check
}

func NewMockService(concepts map[string]ConcordedConcept, notifications []sqs.Notification, healthchecks []fthealth.Check) Service {
	return &MockService{
		concepts:      concepts,
		notifications: notifications,
		healthchecks:  healthchecks,
	}
}

func (s *MockService) ListenForNotifications() {
	for _, n := range s.notifications {
		s.ProcessMessage(n)
	}
}

func (s *MockService) ProcessMessage(notification sqs.Notification) error {
	//s.m.Lock()
	//defer s.m.Unlock()

	if _, _, err := s.GetConcordedConcept(notification.UUID); err != nil {
		return err
	}
	return nil
}

func (s *MockService) GetConcordedConcept(UUID string) (ConcordedConcept, string, error) {
	//s.m.Lock()
	//defer s.m.Unlock()
	if c, ok := s.concepts[UUID]; ok {
		return c, "tid", nil
	}
	return ConcordedConcept{}, "", errors.New("Not found")
}

func (s *MockService) Healthchecks() []fthealth.Check {
	if s.healthchecks != nil {
		return s.healthchecks
	}
	return []fthealth.Check{}
}
