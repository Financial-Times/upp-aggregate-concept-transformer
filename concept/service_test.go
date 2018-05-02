package concept

import (
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/Financial-Times/aggregate-concept-transformer/concordances"
	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	"github.com/stretchr/testify/assert"
)

const (
	payload = `{
    			"UpdatedIds": [
        			"28090964-9997-4bc2-9638-7a11135aaff9",
        			"34a571fb-d779-4610-a7ba-2e127676db4d"
    			]
		 }`
	emptyPayload = `{
    			"UpdatedIds": [

    			]
		 }`
	esUrl            = "concept-rw-elasticsearch"
	neo4jUrl         = "concepts-rw-neo4j"
	varnishPurgerUrl = "varnish-purger"
)

func TestNewService(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	assert.Equal(t, 7, len(svc.Healthchecks()))
}

func TestAggregateService_ListenForNotifications(t *testing.T) {
	svc, _, mockSqsClient, _, _ := setupTestService(200, payload)
	go svc.ListenForNotifications()
	time.Sleep(2 * time.Second)
	assert.Equal(t, 0, len(mockSqsClient.Queue()))
}

func TestAggregateService_ListenForNotifications_CannotProcessConceptNotInS3(t *testing.T) {
	svc, _, mockSqsClient, _, _ := setupTestService(200, payload)
	var receiptHandle string = "1"
	var nonExistingConcept string = "99247059-04ec-3abb-8693-a0b8951fdcab"
	mockSqsClient.queue[receiptHandle] = nonExistingConcept
	var expectedMap = make(map[string]string)
	expectedMap[receiptHandle] = nonExistingConcept
	go svc.ListenForNotifications()
	time.Sleep(50 * time.Microsecond)
	assert.Equal(t, expectedMap, mockSqsClient.queue)
	assert.Equal(t, 1, len(mockSqsClient.Queue()))
	err := mockSqsClient.RemoveMessageFromQueue(&receiptHandle)
	assert.NoError(t, err)
}

func TestAggregateService_ListenForNotifications_CannotProcessRemoveMessageNotPresentOnQueue(t *testing.T) {
	svc, _, mockSqsClient, _, _ := setupTestService(200, payload)
	var receiptHandle string = "2"
	go svc.ListenForNotifications()
	err := mockSqsClient.RemoveMessageFromQueue(&receiptHandle)
	assert.Error(t, err)
	assert.Equal(t, "Receipt handle not present on queue", err.Error())
}

func TestAggregateService_GetConcordedConcept_NoConcordance(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)

	c, tid, err := svc.GetConcordedConcept("99247059-04ec-3abb-8693-a0b8951fdcab")
	assert.NoError(t, err)
	assert.Equal(t, "tid_123", tid)
	assert.Equal(t, "Test Concept", c.PrefLabel)
}

func TestAggregateService_GetConcordedConcept_TMEConcordance(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	expectedConcept := ConcordedConcept{
		PrefUUID:        "28090964-9997-4bc2-9638-7a11135aaff9",
		PrefLabel:       "Root Concept",
		Type:            "Person",
		Aliases:         []string{"TME Concept", "Root Concept"},
		EmailAddress:    "person123@ft.com",
		FacebookPage:    "facebook/smartlogicPerson",
		TwitterHandle:   "@FtSmartlogicPerson",
		ScopeNote:       "This note is in scope",
		ShortLabel:      "Concept",
		InceptionDate:   "2002-06-01",
		TerminationDate: "2011-11-29",
		FigiCode:        "BBG000Y1HJT8",
		IssuedBy:        "613b1f72-cc74-4d8f-9406-28fc91b82a2a",
		MembershipRoles: []MembershipRole{
			{
				RoleUUID:        "ccdff192-4d6c-4539-bbe8-7e24e81ed49e",
				InceptionDate:   "2002-06-01",
				TerminationDate: "2011-11-29",
			},
		},
		OrganisationUUID: "a4528fc9-0615-4bfa-bc99-596ea1ddec28",
		PersonUUID:       "973509c1-5238-4c83-9a7d-89009e839ff8",
		SourceRepresentations: []s3.Concept{
			{
				UUID:      "34a571fb-d779-4610-a7ba-2e127676db4d",
				PrefLabel: "TME Concept",
				Authority: "TME",
				AuthValue: "TME-123",
				Type:      "Person",
			},
			{
				UUID:            "28090964-9997-4bc2-9638-7a11135aaff9",
				PrefLabel:       "Root Concept",
				Authority:       "Smartlogic",
				AuthValue:       "28090964-9997-4bc2-9638-7a11135aaff9",
				Type:            "Person",
				FacebookPage:    "facebook/smartlogicPerson",
				TwitterHandle:   "@FtSmartlogicPerson",
				ScopeNote:       "This note is in scope",
				EmailAddress:    "person123@ft.com",
				ShortLabel:      "Concept",
				InceptionDate:   "2002-06-01",
				TerminationDate: "2011-11-29",
				FigiCode:        "BBG000Y1HJT8",
				IssuedBy:        "613b1f72-cc74-4d8f-9406-28fc91b82a2a",
				MembershipRoles: []s3.MembershipRole{
					{
						RoleUUID:        "ccdff192-4d6c-4539-bbe8-7e24e81ed49e",
						InceptionDate:   "2002-06-01",
						TerminationDate: "2011-11-29",
					},
				},
				OrganisationUUID: "a4528fc9-0615-4bfa-bc99-596ea1ddec28",
				PersonUUID:       "973509c1-5238-4c83-9a7d-89009e839ff8",
			},
		},
	}

	c, tid, err := svc.GetConcordedConcept("28090964-9997-4bc2-9638-7a11135aaff9")
	sort.Strings(c.Aliases)
	sort.Strings(expectedConcept.Aliases)
	assert.NoError(t, err)
	assert.Equal(t, "tid_456", tid)
	assert.Equal(t, expectedConcept, c)
}

func TestAggregateService_GetConcordedConcept_FinancialInstrument(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	expectedConcept := ConcordedConcept{
		PrefUUID:  "6562674e-dbfa-4cb0-85b2-41b0948b7cc2",
		PrefLabel: "Some random finanial instrument",
		Type:      "FinancialInstrument",
		Aliases:   []string{"Some random finanial instrument"},
		FigiCode:  "BBG000Y1HJT8",
		IssuedBy:  "4e484678-cf47-4168-b844-6adb47f8eb58",
		SourceRepresentations: []s3.Concept{
			{
				UUID:      "6562674e-dbfa-4cb0-85b2-41b0948b7cc2",
				PrefLabel: "Some random finanial instrument",
				Authority: "FACTSET",
				AuthValue: "B000BB-S",
				Type:      "FinancialInstrument",
				FigiCode:  "BBG000Y1HJT8",
				IssuedBy:  "4e484678-cf47-4168-b844-6adb47f8eb58",
			},
		},
	}

	c, tid, err := svc.GetConcordedConcept("6562674e-dbfa-4cb0-85b2-41b0948b7cc2")
	sort.Strings(c.Aliases)
	sort.Strings(expectedConcept.Aliases)
	assert.NoError(t, err)
	assert.Equal(t, "tid_630", tid)
	assert.Equal(t, expectedConcept, c)
}

func TestAggregateService_GetConcordedConcept_Organisation(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	expectedConcept := ConcordedConcept{
		PrefUUID:    "c28fa0b4-4245-11e8-842f-0ed5f89f718b",
		Type:        "PublicCompany",
		ProperName:  "Strix Group Plc",
		PrefLabel:   "Strix Group Plc",
		ShortName:   "Strix Group",
		HiddenLabel: "STRIX GROUP PLC",
		FormerNames: []string{
			"Castletown Thermostats",
			"Steam Plc",
		},
		Aliases: []string{
			"Strix Group Plc",
			"STRIX GROUP PLC",
			"Strix Group",
			"Castletown Thermostats",
			"Steam Plc",
		},
		CountryCode:            "GB",
		CountryOfIncorporation: "IM",
		PostalCode:             "IM9 2RG",
		YearFounded:            1951,
		EmailAddress:           "info@strix.com",
		LeiCode:                "213800KZEW5W6BZMNT62",
		SourceRepresentations: []s3.Concept{
			{
				UUID:        "c28fa0b4-4245-11e8-842f-0ed5f89f718b",
				Type:        "PublicCompany",
				Authority:   "FACTSET",
				AuthValue:   "B000BB-S",
				ProperName:  "Strix Group Plc",
				PrefLabel:   "Strix Group Plc",
				ShortName:   "Strix Group",
				HiddenLabel: "STRIX GROUP PLC",
				FormerNames: []string{
					"Castletown Thermostats",
					"Steam Plc",
				},
				Aliases: []string{
					"Strix Group Plc",
					"STRIX GROUP PLC",
					"Strix Group",
					"Castletown Thermostats",
					"Steam Plc",
				},
				CountryCode:            "GB",
				CountryOfIncorporation: "IM",
				PostalCode:             "IM9 2RG",
				YearFounded:            1951,
				EmailAddress:           "info@strix.com",
				LeiCode:                "213800KZEW5W6BZMNT62",
				ParentOrganisation:     "123",
			},
		},
	}
	c, tid, err := svc.GetConcordedConcept("c28fa0b4-4245-11e8-842f-0ed5f89f718b")
	sort.Strings(c.FormerNames)
	sort.Strings(c.Aliases)
	sort.Strings(expectedConcept.FormerNames)
	sort.Strings(expectedConcept.Aliases)
	assert.NoError(t, err)
	assert.Equal(t, "tid_631", tid)
	assert.Equal(t, expectedConcept, c)
}

func TestAggregateService_GetConcordedConcept_BoardRole(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	expectedConcept := ConcordedConcept{
		PrefUUID:  "344fdb1d-0585-31f7-814f-b478e54dbe1f",
		PrefLabel: "Director/Board Member",
		Type:      "BoardRole",
		Aliases:   []string{"Director/Board Member"},
		SourceRepresentations: []s3.Concept{
			{
				UUID:      "344fdb1d-0585-31f7-814f-b478e54dbe1f",
				PrefLabel: "Director/Board Member",
				Authority: "FACTSET",
				AuthValue: "BRD",
				Type:      "BoardRole",
			},
		},
	}

	c, tid, err := svc.GetConcordedConcept("344fdb1d-0585-31f7-814f-b478e54dbe1f")
	sort.Strings(c.Aliases)
	sort.Strings(expectedConcept.Aliases)
	assert.NoError(t, err)
	assert.Equal(t, "tid_631", tid)
	assert.Equal(t, expectedConcept, c)
}

func TestAggregateService_GetConcordedConcept_Memberships(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	expectedConcept := ConcordedConcept{
		PrefUUID:         "87cda39a-e354-3dfb-b28a-b9a04887577b",
		PrefLabel:        "Independent Non-Executive Director",
		Type:             "Membership",
		Aliases:          []string{"Independent Non-Executive Director"},
		PersonUUID:       "d4050b35-45ac-3933-9fad-7720a0dce8df",
		OrganisationUUID: "064ce159-8835-3426-b456-c86d48de8511",
		InceptionDate:    "2002-06-01",
		TerminationDate:  "2011-11-30",
		MembershipRoles: []MembershipRole{
			{

				RoleUUID:        "344fdb1d-0585-31f7-814f-b478e54dbe1f",
				InceptionDate:   "2002-06-01",
				TerminationDate: "2011-11-29",
			},
			{
				RoleUUID:        "abacb0e1-3f7e-334a-96b9-ed5da35f3251",
				InceptionDate:   "2011-07-26",
				TerminationDate: "2011-11-29",
			},
		},
		SourceRepresentations: []s3.Concept{
			{
				UUID:             "87cda39a-e354-3dfb-b28a-b9a04887577b",
				PrefLabel:        "Independent Non-Executive Director",
				Authority:        "FACTSET",
				AuthValue:        "1000016",
				Type:             "Membership",
				PersonUUID:       "d4050b35-45ac-3933-9fad-7720a0dce8df",
				OrganisationUUID: "064ce159-8835-3426-b456-c86d48de8511",
				InceptionDate:    "2002-06-01",
				TerminationDate:  "2011-11-30",
				MembershipRoles: []s3.MembershipRole{
					{

						RoleUUID:        "344fdb1d-0585-31f7-814f-b478e54dbe1f",
						InceptionDate:   "2002-06-01",
						TerminationDate: "2011-11-29",
					},
					{
						RoleUUID:        "abacb0e1-3f7e-334a-96b9-ed5da35f3251",
						InceptionDate:   "2011-07-26",
						TerminationDate: "2011-11-29",
					},
				},
			},
		},
	}

	c, tid, err := svc.GetConcordedConcept("87cda39a-e354-3dfb-b28a-b9a04887577b")
	sort.Strings(c.Aliases)
	sort.Strings(expectedConcept.Aliases)
	assert.NoError(t, err)
	assert.Equal(t, "tid_632", tid)
	assert.Equal(t, expectedConcept, c)
}

func TestAggregateService_ProcessMessage_Success(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	mockWriter := svc.(*AggregateService).httpClient.(*mockHTTPClient)
	assert.Equal(t, []string{
		"concepts-rw-neo4j/people/28090964-9997-4bc2-9638-7a11135aaff9",
		"varnish-purger/purge?target=%2Fpeople%2F28090964-9997-4bc2-9638-7a11135aaff9&target=%2Fthings%2F28090964-9997-4bc2-9638-7a11135aaff9&target=%2Fpeople%2F34a571fb-d779-4610-a7ba-2e127676db4d&target=%2Fthings%2F34a571fb-d779-4610-a7ba-2e127676db4d",
		"concept-rw-elasticsearch/people/28090964-9997-4bc2-9638-7a11135aaff9",
	}, mockWriter.called)
	assert.NoError(t, err)
}

func TestAggregateService_ProcessMessage_NoElasticSuccess(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	err := svc.ProcessMessage("6562674e-dbfa-4cb0-85b2-41b0948b7cc2")
	mockWriter := svc.(*AggregateService).httpClient.(*mockHTTPClient)
	assert.Equal(t, []string{
		"concepts-rw-neo4j/financial-instruments/6562674e-dbfa-4cb0-85b2-41b0948b7cc2",
	}, mockWriter.called)
	assert.NoError(t, err)
}

func TestAggregateService_ProcessMessage_GenericDynamoError(t *testing.T) {
	svc, _, _, mockDynamoClient, _ := setupTestService(200, payload)
	mockDynamoClient.err = errors.New("Could not get concordance record from DynamoDB")
	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Could not get concordance record from DynamoDB", err.Error())
}

func TestAggregateService_ProcessMessage_GenericS3Error(t *testing.T) {
	svc, mockS3Client, _, _, _ := setupTestService(200, payload)
	mockS3Client.err = errors.New("Error retrieving concept from S3")
	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Error retrieving concept from S3", err.Error())
}

func TestAggregateService_ProcessMessage_GenericWriterError(t *testing.T) {
	svc, _, _, _, _ := setupTestService(503, payload)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Request to concepts-rw-neo4j/people/28090964-9997-4bc2-9638-7a11135aaff9 returned status: 503; skipping 28090964-9997-4bc2-9638-7a11135aaff9", err.Error())
}

func TestAggregateService_ProcessMessage_GenericKinesisError(t *testing.T) {
	svc, _, _, _, mockKinesisClient := setupTestService(200, payload)
	mockKinesisClient.err = errors.New("Failed to add record to stream")

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.Error(t, err)
	assert.Equal(t, "Failed to add record to stream", err.Error())
}

func TestAggregateService_ProcessMessage_S3SourceNotFound(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	err := svc.ProcessMessage("c9d3a92a-da84-11e7-a121-0401beb96201")
	assert.Error(t, err)
	assert.Equal(t, "Source concept 3a3da730-0f4c-4a20-85a6-3ebd5776bd49 not found in S3", err.Error())
}

func TestAggregateService_ProcessMessage_S3CanonicalNotFound(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
	err := svc.ProcessMessage("45f278ef-91b2-45f7-9545-fbc79c1b4004")
	assert.Error(t, err)
	assert.Equal(t, "Canonical concept 45f278ef-91b2-45f7-9545-fbc79c1b4004 not found in S3", err.Error())
}

func TestAggregateService_ProcessMessage_WriterReturnsNoUuids(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, emptyPayload)

	err := svc.ProcessMessage("28090964-9997-4bc2-9638-7a11135aaff9")
	assert.NoError(t, err)
}

func TestAggregateService_Healthchecks(t *testing.T) {
	svc, _, _, _, _ := setupTestService(200, payload)
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

	financialInstrument := resolveConceptType("FinancialInstrument")
	assert.Equal(t, "financial-instruments", financialInstrument)

	alphavilleSeries := resolveConceptType("AlphavilleSeries")
	assert.Equal(t, "alphaville-series", alphavilleSeries)

	topic := resolveConceptType("Topic")
	assert.Equal(t, "topics", topic)
}

func setupTestService(httpError int, writerResponse string) (Service, *mockS3Client, *mockSQSClient, *mockConcordancesClient, *mockKinesisStreamClient) {
	s3 := &mockS3Client{
		concepts: map[string]struct {
			transactionID string
			concept       s3.Concept
		}{
			"c28fa0b4-4245-11e8-842f-0ed5f89f718b": {
				transactionID: "tid_631",
				concept: s3.Concept{
					UUID:        "c28fa0b4-4245-11e8-842f-0ed5f89f718b",
					Type:        "PublicCompany",
					Authority:   "FACTSET",
					AuthValue:   "B000BB-S",
					ProperName:  "Strix Group Plc",
					PrefLabel:   "Strix Group Plc",
					ShortName:   "Strix Group",
					HiddenLabel: "STRIX GROUP PLC",
					FormerNames: []string{
						"Castletown Thermostats",
						"Steam Plc",
					},
					Aliases: []string{
						"Strix Group Plc",
						"STRIX GROUP PLC",
						"Strix Group",
						"Castletown Thermostats",
						"Steam Plc",
					},
					CountryCode:            "GB",
					CountryOfIncorporation: "IM",
					PostalCode:             "IM9 2RG",
					YearFounded:            1951,
					EmailAddress:           "info@strix.com",
					LeiCode:                "213800KZEW5W6BZMNT62",
					ParentOrganisation:     "123",
				},
			},
			"99247059-04ec-3abb-8693-a0b8951fdcab": {
				transactionID: "tid_123",
				concept: s3.Concept{
					UUID:      "99247059-04eFc-3abb-8693-a0b8951fdcab",
					PrefLabel: "Test Concept",
					Authority: "Smartlogic",
					AuthValue: "99247059-04ec-3abb-8693-a0b8951fdcab",
					Type:      "Person",
				},
			},
			"28090964-9997-4bc2-9638-7a11135aaff9": {
				transactionID: "tid_456",
				concept: s3.Concept{
					UUID:          "28090964-9997-4bc2-9638-7a11135aaff9",
					PrefLabel:     "Root Concept",
					Authority:     "Smartlogic",
					AuthValue:     "28090964-9997-4bc2-9638-7a11135aaff9",
					Type:          "Person",
					FacebookPage:  "facebook/smartlogicPerson",
					TwitterHandle: "@FtSmartlogicPerson",
					ScopeNote:     "This note is in scope",
					EmailAddress:  "person123@ft.com",
					ShortLabel:    "Concept",
					MembershipRoles: []s3.MembershipRole{
						{
							RoleUUID:        "ccdff192-4d6c-4539-bbe8-7e24e81ed49e",
							InceptionDate:   "2002-06-01",
							TerminationDate: "2011-11-29",
						},
					},
					OrganisationUUID: "a4528fc9-0615-4bfa-bc99-596ea1ddec28",
					PersonUUID:       "973509c1-5238-4c83-9a7d-89009e839ff8",
					InceptionDate:    "2002-06-01",
					TerminationDate:  "2011-11-29",
					FigiCode:         "BBG000Y1HJT8",
					IssuedBy:         "613b1f72-cc74-4d8f-9406-28fc91b82a2a",
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
			"c9d3a92a-da84-11e7-a121-0401beb96201": {
				transactionID: "tid_629",
				concept: s3.Concept{
					UUID:      "c9d3a92a-da84-11e7-a121-0401beb96201",
					PrefLabel: "TME Concept",
					Authority: "TME",
					AuthValue: "TME-a2f",
					Type:      "Person",
				},
			},
			"6562674e-dbfa-4cb0-85b2-41b0948b7cc2": {
				transactionID: "tid_630",
				concept: s3.Concept{
					UUID:      "6562674e-dbfa-4cb0-85b2-41b0948b7cc2",
					PrefLabel: "Some random finanial instrument",
					Authority: "FACTSET",
					AuthValue: "B000BB-S",
					Type:      "FinancialInstrument",
					FigiCode:  "BBG000Y1HJT8",
					IssuedBy:  "4e484678-cf47-4168-b844-6adb47f8eb58",
				},
			},
			"344fdb1d-0585-31f7-814f-b478e54dbe1f": {
				transactionID: "tid_631",
				concept: s3.Concept{
					UUID:      "344fdb1d-0585-31f7-814f-b478e54dbe1f",
					PrefLabel: "Director/Board Member",
					Authority: "FACTSET",
					AuthValue: "BRD",
					Type:      "BoardRole",
				},
			},
			"87cda39a-e354-3dfb-b28a-b9a04887577b": {
				transactionID: "tid_632",
				concept: s3.Concept{
					UUID:             "87cda39a-e354-3dfb-b28a-b9a04887577b",
					PrefLabel:        "Independent Non-Executive Director",
					Authority:        "FACTSET",
					AuthValue:        "1000016",
					Type:             "Membership",
					PersonUUID:       "d4050b35-45ac-3933-9fad-7720a0dce8df",
					OrganisationUUID: "064ce159-8835-3426-b456-c86d48de8511",
					InceptionDate:    "2002-06-01",
					TerminationDate:  "2011-11-30",
					MembershipRoles: []s3.MembershipRole{
						{

							RoleUUID:        "344fdb1d-0585-31f7-814f-b478e54dbe1f",
							InceptionDate:   "2002-06-01",
							TerminationDate: "2011-11-29",
						},
						{
							RoleUUID:        "abacb0e1-3f7e-334a-96b9-ed5da35f3251",
							InceptionDate:   "2011-07-26",
							TerminationDate: "2011-11-29",
						},
					},
				},
			},
		},
	}
	sqs := &mockSQSClient{
		queue: map[string]string{
			"1": "99247059-04ec-3abb-8693-a0b8951fdcab",
		},
	}
	concordClient := &mockConcordancesClient{
		concordances: map[string][]concordances.ConcordanceRecord{
			"28090964-9997-4bc2-9638-7a11135aaff9": []concordances.ConcordanceRecord{
				concordances.ConcordanceRecord{
					UUID:      "28090964-9997-4bc2-9638-7a11135aaff9",
					Authority: "SmartLogic",
				},
				concordances.ConcordanceRecord{
					UUID:      "34a571fb-d779-4610-a7ba-2e127676db4d",
					Authority: "FT-TME",
				},
			},
			"c9d3a92a-da84-11e7-a121-0401beb96201": []concordances.ConcordanceRecord{
				concordances.ConcordanceRecord{
					UUID:      "c9d3a92a-da84-11e7-a121-0401beb96201",
					Authority: "SmartLogic",
				},
				concordances.ConcordanceRecord{
					UUID:      "3a3da730-0f4c-4a20-85a6-3ebd5776bd49",
					Authority: "FT-TME",
				},
			},
			"4a4aaca0-b059-426c-bf4f-f00c6ef940ae": []concordances.ConcordanceRecord{
				concordances.ConcordanceRecord{
					UUID:      "4a4aaca0-b059-426c-bf4f-f00c6ef940ae",
					Authority: "SmartLogic",
				},
				concordances.ConcordanceRecord{
					UUID:      "3a3da730-0f4c-4a20-85a6-3ebd5776bd49",
					Authority: "FT-TME",
				},
			},
		},
	}

	kinesis := &mockKinesisStreamClient{}

	return NewService(s3, sqs, concordClient, kinesis,
		neo4jUrl,
		esUrl,
		varnishPurgerUrl,
		&mockHTTPClient{
			resp:       writerResponse,
			statusCode: httpError,
			err:        nil,
			called:     []string{},
		}, 1,
	), s3, sqs, concordClient, kinesis
}
