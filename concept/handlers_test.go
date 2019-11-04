package concept

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sync"

	"github.com/Financial-Times/aggregate-concept-transformer/sqs"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestHandlers(t *testing.T) {
	testCases := map[string]struct {
		method        string
		url           string
		requestBody   string
		resultCode    int
		resultBody    string
		err           error
		concepts      map[string]ConcordedConcept
		notifications []sqs.ConceptUpdate
		healthchecks  []fthealth.Check
		cancelContext bool
	}{
		"Get Concept - Success": {
			method:     "GET",
			url:        "/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097",
			resultCode: 200,
			resultBody: "{\"prefUUID\":\"f7fd05ea-9999-47c0-9be9-c99dd84d0097\",\"prefLabel\":\"TestConcept\"}\n",
			concepts: map[string]ConcordedConcept{
				"f7fd05ea-9999-47c0-9be9-c99dd84d0097": {
					PrefUUID:  "f7fd05ea-9999-47c0-9be9-c99dd84d0097",
					PrefLabel: "TestConcept",
				},
			},
		},
		"Get Concept - Not Found": {
			method:     "GET",
			url:        "/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097",
			resultCode: 500,
			resultBody: "{\"message\":\"Canonical concept not found in S3\"}",
			err:        errors.New("Canonical concept not found in S3"),
		},
		"Send Concept - Success": {
			method:     "POST",
			url:        "/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097/send",
			resultCode: 200,
			resultBody: "{\"message\":\"Concept f7fd05ea-9999-47c0-9be9-c99dd84d0097 updated successfully.\"}",
			concepts: map[string]ConcordedConcept{
				"f7fd05ea-9999-47c0-9be9-c99dd84d0097": {
					PrefUUID:  "f7fd05ea-9999-47c0-9be9-c99dd84d0097",
					PrefLabel: "TestConcept",
				},
			},
		},
		"Send Concept - Failure": {
			method:     "POST",
			url:        "/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097/send",
			resultCode: 500,
			resultBody: "{\"message\":\"Could not process the concept.\"}",
			err:        errors.New("Could not process the concept."),
		},
		"GTG - Success": {
			method:     "GET",
			url:        "/__gtg",
			resultCode: 200,
			resultBody: "OK",
		},
		"GTG - Failure": {
			method:     "GET",
			url:        "/__gtg",
			resultCode: 503,
			resultBody: "GTG fail error",
			healthchecks: []fthealth.Check{
				{
					Checker: func() (string, error) {
						return "", errors.New("GTG fail error")
					},
				},
			},
		},
		"Get Concept - Context cancelled": {
			method:        "GET",
			url:           "/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097",
			resultCode:    500,
			resultBody:    "{\"message\":\"context canceled\"}",
			cancelContext: true,
		},
		"Send Concept - Context cancelled": {
			method:        "POST",
			url:           "/concept/f7fd05ea-9999-47c0-9be9-c99dd84d0097/send",
			resultCode:    500,
			resultBody:    "{\"message\":\"context canceled\"}",
			cancelContext: true,
		},
	}

	for testName, d := range testCases {
		t.Run(testName, func(t *testing.T) {
			fb := make(chan bool)
			mockService := NewMockService(d.concepts, d.notifications, d.healthchecks, d.err)
			handler := NewHandler(mockService, time.Second*1)
			m := mux.NewRouter()
			handler.RegisterHandlers(m)
			handler.RegisterAdminHandlers(m, NewHealthService(mockService, "system-code", "app-name", 8080, "description"), true, fb)

			ctx, cancel := context.WithCancel(context.Background())
			if d.cancelContext {
				cancel()
			} else {
				defer cancel()
			}

			req, _ := http.NewRequestWithContext(ctx, d.method, d.url, bytes.NewBufferString(d.requestBody))
			rr := httptest.NewRecorder()

			m.ServeHTTP(rr, req)

			b, err := ioutil.ReadAll(rr.Body)
			assert.NoError(t, err)
			body := string(b)
			assert.Equal(t, d.resultCode, rr.Code, testName)
			if d.resultBody != "IGNORE" {
				assert.Equal(t, d.resultBody, body, testName)
			}
		})
	}
}

type MockService struct {
	notifications []sqs.ConceptUpdate
	concepts      map[string]ConcordedConcept
	m             sync.RWMutex
	healthchecks  []fthealth.Check
	err           error
}

func NewMockService(concepts map[string]ConcordedConcept, notifications []sqs.ConceptUpdate, healthchecks []fthealth.Check, err error) Service {
	return &MockService{
		concepts:      concepts,
		notifications: notifications,
		healthchecks:  healthchecks,
		err:           err,
	}
}

func (s *MockService) ListenForNotifications(ctx context.Context, workerId int) {
	for _, n := range s.notifications {
		//nolint:errcheck
		s.ProcessMessage(ctx, n.UUID, n.Bookmark)
	}
}

func (s *MockService) ProcessMessage(ctx context.Context, UUID string, bookmark string) error {
	if _, _, err := s.GetConcordedConcept(ctx, UUID, bookmark); err != nil {
		return err
	}
	return nil
}

func (s *MockService) GetConcordedConcept(ctx context.Context, UUID string, bookmark string) (ConcordedConcept, string, error) {
	if s.err != nil {
		return ConcordedConcept{}, "", s.err
	}
	if c, ok := s.concepts[UUID]; ok {
		return c, "tid", nil
	}
	return ConcordedConcept{}, "", s.err
}

func (s *MockService) Healthchecks() []fthealth.Check {
	if s.healthchecks != nil {
		return s.healthchecks
	}
	return []fthealth.Check{}
}
