package concordances

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	logger "github.com/Financial-Times/go-logger"
)

var (
	ErrNotFound = errors.New("concordances not found")
)

type Client interface {
	GetConcordance(uuid string) ([]ConcordanceRecord, error)
	Healthcheck() fthealth.Check
}

type RWClient struct {
	address    *url.URL
	httpClient *http.Client
}

func NewClient(address string) (Client, error) {
	url, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	return &RWClient{
		address:    url,
		httpClient: http.DefaultClient,
	}, nil
}

func (c *RWClient) GetConcordance(uuid string) ([]ConcordanceRecord, error) {
	respBody, status, err := c.makeRequest("GET", fmt.Sprintf("/concordances/%s", uuid), nil)
	if err != nil {
		logger.WithError(err).Error("Could not get concordances")
		return nil, err
	}

	if status == http.StatusNotFound {
		// No concordance found, so we'll create a fake record to return the solo concept.
		logger.WithError(err).WithField("UUID", uuid).Debug("No matching record in db")
		return []ConcordanceRecord{
			ConcordanceRecord{
				UUID:      uuid,
				Authority: "SmartLogic",
			},
		}, nil

	}

	if status != http.StatusOK {
		logger.WithError(err).WithField("status", status).Error("Could not get concordances, invalid status")
		return nil, errors.New("Invalid status response")
	}

	cons := []ConcordanceRecord{}
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
		TechnicalSummary: "The concordance-rw-neo4j service is inaccessible.  Check that the address is correct and " +
			"the service is up.",
		Timeout: 10 * time.Second,
		Checker: func() (string, error) {
			_, status, err := c.makeRequest("GET", "/__gtg", nil)
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

func (c *RWClient) makeRequest(method string, path string, body []byte) ([]byte, int, error) {
	finalURL := *c.address
	finalURL.Path = path

	req, err := http.NewRequest(method, finalURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	return respBody, resp.StatusCode, err
}
