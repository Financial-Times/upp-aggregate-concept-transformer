package neo4j

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
)

type Client interface {
	GetConcordance(uuid string) ([]ConcordanceRecord, error)
	Healthcheck() fthealth.Check
}

type RWClient struct {
	address *url.URL
	httpClient *http.Client
}

func NewClient(address string) (Client, error) {
	url, err := url.Parse(address)
	if err != nil{
		return &RWClient{}, err
	}
	return &RWClient{
		address: url,
		httpClient: http.DefaultClient,
	}, nil
}


func (c *RWClient) GetConcordance(uuid string) ([]ConcordanceRecord, error) {
	//
	//respBody, status, err := c.makeRequest("GET", fmt.Sprintf("/concordances/%s", uuid), nil)
	//if err != nil {
	//	logger.WithError(err).Error()
	//}

	return []ConcordanceRecord{}, nil


}

func (c *RWClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Name: "Concordance store is accessible",
		BusinessImpact: "Concordances cannot be returned",
		ID: "concordance-store-rw-check",
		Severity: 3,
		PanicGuide: "https://dewey.in.ft.com/view/system/aggregate-concept-transformer",
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

func (c *RWClient) makeRequest(method string, path string, body []byte) (string, int, error) {
	finalURL := *c.address
	finalURL.Path = path

	req, err := http.NewRequest(method, finalURL.String(), bytes.NewReader(body))
	if err != nil{
		return "", 0, err
	}

	resp, err := c.httpClient.Do(req)
	defer resp.Body.Close()
	if err != nil{
		return "", 0, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)

	return string(respBody), resp.StatusCode, err
}