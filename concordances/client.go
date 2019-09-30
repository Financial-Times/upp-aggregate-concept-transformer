package concordances

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
)

type Client interface {
	GetConcordance(ctx context.Context, uuid string, bookmark string) ([]ConcordanceRecord, error)
	Healthcheck() fthealth.Check
}

type RWClient struct {
	address    *url.URL
	httpClient *http.Client
}

func NewClient(address string) (Client, error) {
	parsedURL, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	return &RWClient{
		address: parsedURL,
		httpClient: &http.Client{
			Timeout: time.Second * 5,
		},
	}, nil
}

func (c *RWClient) GetConcordance(ctx context.Context, uuid string, bookmark string) ([]ConcordanceRecord, error) {
	respBody, status, err := c.makeRequest(ctx, "GET", fmt.Sprintf("/concordances/%s", uuid), nil, bookmark)
	if err != nil {
		logger.WithError(err).Error("Could not get concordances")
		return nil, err
	}

	if status == http.StatusNotFound {
		// No concordance found, so we'll create a fake record to return the solo concept.
		logger.WithError(err).WithField("UUID", uuid).Debug("No matching record in db")
		return []ConcordanceRecord{
			{
				UUID:      uuid,
				Authority: "Smartlogic", //we have to provide a primary authority here, but it will be indifferent at a later point if this is Smartlogic or ManagedLocation
			},
		}, nil

	}

	if status != http.StatusOK {
		logger.WithError(err).WithField("status", status).Error("Could not get concordances, invalid status")
		return nil, errors.New("invalid status response")
	}

	var cons []ConcordanceRecord
	if err := json.Unmarshal(respBody, &cons); err != nil {
		return nil, err
	}

	return cons, nil
}

func (c *RWClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Name:           "Concordance store is accessible",
		BusinessImpact: "Concordances cannot be returned",
		ID:             "concordance-store-rw-check",
		Severity:       3,
		PanicGuide:     "https://dewey.in.ft.com/view/system/aggregate-concept-transformer",
		TechnicalSummary: "The concordances-rw-neo4j service is inaccessible.  Check that the address is correct and " +
			"the service is up.",
		Timeout: 10 * time.Second,
		Checker: func() (string, error) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, status, err := c.makeRequest(ctx, "GET", "/__gtg", nil, "")
			if err != nil {
				errMsg := "failed to request gtg from concordances-rw-neo4j"
				return errMsg, errors.New(errMsg)
			}
			if status != 200 {
				errMsg := "bad status from gtg for concordances-rw-neo4j"
				return errMsg, errors.New(errMsg)
			}
			return "", nil
		},
	}
}

func (c *RWClient) makeRequest(ctx context.Context, method string, path string, body []byte, bookmark string) ([]byte, int, error) {
	finalURL := *c.address
	finalURL.Path = finalURL.Path + path

	req, err := http.NewRequestWithContext(ctx, method, finalURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}

	if bookmark != "" {
		req.Header.Add("bookmark", bookmark)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer func() {
		if err = resp.Body.Close(); err != nil {
			logger.WithError(err).Error("Could not close body..")
		}
	}()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return respBody, resp.StatusCode, err
}
