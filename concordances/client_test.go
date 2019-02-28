package concordances

import (
	"testing"

	"github.com/Financial-Times/go-logger"

	"github.com/stretchr/testify/suite"
	"gopkg.in/jarcoal/httpmock.v1"
)

func init() {
	logger.InitDefaultLogger("test")
}

type RWTestSuite struct {
	suite.Suite
	client *RWClient
}

func (suite *RWTestSuite) SetupTest() {
	client, err := NewClient("http://localhost")
	suite.Nil(err)
	suite.client = client.(*RWClient)
}

func (suite *RWTestSuite) TestGetConcordance_Success() {
	httpmock.RegisterResponder(
		"GET",
		"http://localhost/concordances/a",
		httpmock.NewStringResponder(200, `[{"uuid": "a"}, {"uuid": "b"}]`),
	)

	cs, err := suite.client.GetConcordance("a", "")
	suite.Nil(err)
	suite.Len(cs, 2)
	suite.Equal("a", cs[0].UUID)
	suite.Equal("b", cs[1].UUID)
}

func (suite *RWTestSuite) TestGetConcordance_FailOnStatus() {
	httpmock.RegisterResponder(
		"GET",
		"http://localhost/concordances/a",
		httpmock.NewStringResponder(500, `[{"uuid": "a"}, {"uuid": "b"}]`),
	)

	cs, err := suite.client.GetConcordance("a", "")
	suite.Nil(cs)
	suite.NotNil(err)
}

func (suite *RWTestSuite) TestGetConcordance_FailOnInvalidJSON() {
	httpmock.RegisterResponder(
		"GET",
		"http://localhost/concordances/a",
		httpmock.NewStringResponder(200, `...`),
	)

	cs, err := suite.client.GetConcordance("a","")
	suite.Nil(cs)
	suite.NotNil(err)
}

func (suite *RWTestSuite) TestGetConcordance_MissingConcordanceReturns404() {
	httpmock.RegisterResponder(
		"GET",
		"http://localhost/concordances/a",
		httpmock.NewStringResponder(404, `{}`),
	)

	retCon := []ConcordanceRecord{
		ConcordanceRecord{
			UUID:      "a",
			Authority: "Smartlogic",
		},
	}

	cs, err := suite.client.GetConcordance("a","")
	suite.Equal(retCon, cs)
	suite.Nil(err)
}

func (suite *RWTestSuite) TestGetConcordance_FailsOnClientError() {
	cs, err := suite.client.GetConcordance("a","")
	suite.Nil(cs)
	suite.NotNil(err)
}

func (suite *RWTestSuite) TestCheckHealth_Success() {
	httpmock.RegisterResponder(
		"GET",
		"http://localhost/__gtg",
		httpmock.NewStringResponder(200, `{}`),
	)

	status, err := suite.client.Healthcheck().Checker()
	suite.Nil(err)
	suite.Equal("", status)
}

func (suite *RWTestSuite) TestCheckHealth_FailsOnNon200() {
	httpmock.RegisterResponder(
		"GET",
		"http://localhost/__gtg",
		httpmock.NewStringResponder(500, `{}`),
	)

	status, err := suite.client.Healthcheck().Checker()
	suite.NotNil(err)
	suite.Contains(status, "bad status")
}

func (suite *RWTestSuite) TestCheckHealth_FailsOnClientErr() {
	status, err := suite.client.Healthcheck().Checker()
	suite.NotNil(err)
	suite.Contains(status, "failed to request")
}

func TestHandlersTestSuite(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	logger.InitDefaultLogger("concordance-test")

	suite.Run(t, new(RWTestSuite))
}
