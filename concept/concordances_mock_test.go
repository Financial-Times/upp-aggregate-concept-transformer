package concept

import (
	"context"

	"github.com/Financial-Times/aggregate-concept-transformer/concordances"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
)

type mockConcordancesClient struct {
	concordances map[string][]concordances.ConcordanceRecord
	err          error
}

func (d *mockConcordancesClient) GetConcordance(ctx context.Context, uuid string, bookmark string) ([]concordances.ConcordanceRecord, error) {
	if cons, ok := d.concordances[uuid]; ok {
		return cons, d.err
	}
	return []concordances.ConcordanceRecord{
		{
			UUID:      uuid,
			Authority: "Smartlogic",
		},
	}, d.err
}

func (d *mockConcordancesClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}
