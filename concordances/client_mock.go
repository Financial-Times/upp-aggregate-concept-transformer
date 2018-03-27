// Code generated by mockery v1.0.0
package concordances

import mock "github.com/stretchr/testify/mock"
import v1_1 "github.com/Financial-Times/go-fthealth/v1_1"

// MockClient is an autogenerated mock type for the Client type
type MockClient struct {
	mock.Mock
}

// GetConcordance provides a mock function with given fields: uuid
func (_m *MockClient) GetConcordance(uuid string) ([]ConcordanceRecord, error) {
	ret := _m.Called(uuid)

	var r0 []ConcordanceRecord
	if rf, ok := ret.Get(0).(func(string) []ConcordanceRecord); ok {
		r0 = rf(uuid)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]ConcordanceRecord)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(uuid)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Healthcheck provides a mock function with given fields:
func (_m *MockClient) Healthcheck() v1_1.Check {
	ret := _m.Called()

	var r0 v1_1.Check
	if rf, ok := ret.Get(0).(func() v1_1.Check); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(v1_1.Check)
	}

	return r0
}