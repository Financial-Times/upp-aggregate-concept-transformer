package concept

import (
	"context"

	"github.com/Financial-Times/aggregate-concept-transformer/s3"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
)

type mockS3Client struct {
	concepts map[string]struct {
		transactionID string
		concept       s3.Concept
	}
	err error
}

func (s *mockS3Client) GetConceptAndTransactionID(ctx context.Context, UUID string) (bool, s3.Concept, string, error) {
	if c, ok := s.concepts[UUID]; ok {
		return true, c.concept, c.transactionID, s.err
	}
	return false, s3.Concept{}, "", s.err
}
func (s *mockS3Client) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}
