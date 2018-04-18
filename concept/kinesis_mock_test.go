package concept

import fthealth "github.com/Financial-Times/go-fthealth/v1_1"

type mockKinesisStreamClient struct {
	err error
}

func (k *mockKinesisStreamClient) AddRecordToStream(concept []byte, conceptType string) error {
	if k.err != nil {
		return k.err
	}
	return nil
}

func (d *mockKinesisStreamClient) Healthcheck() fthealth.Check {
	return fthealth.Check{
		Checker: func() (string, error) {
			return "", nil
		},
	}
}
