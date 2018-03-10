package promtest

import (
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestRegistry is a prometheus registry meant to be used for testing
type TestRegistry struct {
	*prometheus.Registry
	t *testing.T
}

// NewTestRegistry allocates and initializes a new TestRegistry
func NewTestRegistry(t *testing.T) *TestRegistry {
	return &TestRegistry{
		Registry: prometheus.NewPedanticRegistry(),
		t:        t,
	}
}

// TakeSnapshot takes a snapshot of the current values of metrics for testing
func (r *TestRegistry) TakeSnapshot() (*Snapshot, error) {
	metrics, err := r.Registry.Gather()
	if err != nil {
		return nil, err
	}
	metricMap := make(map[string]*dto.MetricFamily)
	for _, metric := range metrics {
		metricMap[metric.GetName()] = metric
	}
	return &Snapshot{metricMap, r.t}, nil
}

// Snapshot provides methods for asserting on metrics
type Snapshot struct {
	MetricMap map[string]*dto.MetricFamily
	t         *testing.T
}

// AssertCount asserts existence and count of a counter in the snapshot.
func (s *Snapshot) AssertCount(name string, labels map[string]string, value float64) {
	s.t.Helper()
	metric := s.GetMetric(dto.MetricType_COUNTER, name, labels)

	if metric == nil {
		if value == 0 {
			// Counter not existing is the same as the counter having 0 value
			return
		}
		s.t.Errorf("Could not find Counter %s with the labels %v", name, labels)
	}

	if actualValue := metric.GetCounter().GetValue(); !floatEquals(actualValue, value) {
		s.t.Errorf("Expected counter value %f but was %f", value, actualValue)
	}
}

// AssertGauge asserts existence and value of a gauge in the snapshot.
func (s *Snapshot) AssertGauge(name string, labels map[string]string, value float64) {
	s.t.Helper()
	metric := s.GetMetric(dto.MetricType_GAUGE, name, labels)

	if metric == nil {
		if value == 0 {
			// Gauge not existing is the same as the counter having 0 value
			return
		}
		s.t.Errorf("Could not find Gauge %s with the labels %v", name, labels)
	}

	if actualValue := metric.GetGauge().GetValue(); !floatEquals(actualValue, value) {
		s.t.Errorf("Expected gauge value %f but was %f", value, actualValue)
	}
}

// AssertSummary asserts that the existence and the sample sum and count of a summary in the snapshot.
func (s *Snapshot) AssertSummary(name string, labels map[string]string, sum float64, count uint64) {
	s.t.Helper()
	metric := s.GetMetric(dto.MetricType_SUMMARY, name, labels)
	summary := metric.GetSummary()

	if metric == nil {
		if count == 0 {
			// Summary not existing is the same as the summary having 0 value
			return
		}
		s.t.Errorf("Could not find Summary %s with the labels %v", name, labels)
	}

	if actualSum := summary.GetSampleSum(); !floatEquals(actualSum, sum) {
		s.t.Errorf("Expected summary [%s] sample sum to be %f but was %f", name, sum, actualSum)
	}
	if actualCount := summary.GetSampleCount(); actualCount != count {
		s.t.Errorf("Expected summary [%s] sample count to be %d but was %d", name, count, actualCount)
	}
}

// AssertHistogram asserts that the existence and the sample sum and count of a histogram in the
// snapshot.
func (s *Snapshot) AssertHistogram(name string, labels map[string]string, sum float64, count uint64) {
	s.t.Helper()
	metric := s.GetMetric(dto.MetricType_HISTOGRAM, name, labels)
	histogram := metric.GetHistogram()

	if metric == nil {
		if count == 0 {
			// Histogram not existing is the same as the histogram having 0 value
			return
		}
		s.t.Errorf("Could not find Histogram %s with the labels %v", name, labels)
	}

	if actualSum := histogram.GetSampleSum(); !floatEquals(actualSum, sum) {
		s.t.Errorf("Expected histogram [%s] sample sum to be %f but was %f", name, sum, actualSum)
	}
	if actualCount := histogram.GetSampleCount(); actualCount != count {
		s.t.Errorf("Expected histogram [%s] sample count to be %d but was %d", name, count, actualCount)
	}
}

// AssertSummaryNonZero asserts that the summary exists and its value is non-zero
func (s *Snapshot) AssertSummaryNonZero(name string, labels map[string]string) {
	s.t.Helper()
	metric := s.GetMetric(dto.MetricType_SUMMARY, name, labels)
	summary := metric.GetSummary()

	if metric == nil {
		s.t.Errorf("Could not find Summary %s with the labels %v", name, labels)
	}

	if actualSum := summary.GetSampleSum(); actualSum == 0 {
		s.t.Errorf("Expected summary sample sum to be >0")
	}
}

// AssertHistogramSampleCount asserts that the histogram exists and contains exact number of samples
func (s *Snapshot) AssertHistogramSampleCount(name string, sampleCount uint64) {
	s.t.Helper()
	metric := s.GetMetric(dto.MetricType_HISTOGRAM, name, map[string]string{})
	histogram := metric.GetHistogram()

	if histogram == nil {
		s.t.Errorf("Could not find Histogram %s", name)
	}

	if sampleCount != histogram.GetSampleCount() {
		s.t.Errorf("Expected histogram sample count did not match: %d != %d",
			sampleCount, histogram.GetSampleCount())
	}
}

// GetMetric returns a matching metric from the snapshot
func (s *Snapshot) GetMetric(metricType dto.MetricType, name string, labels map[string]string) *dto.Metric {
	family, ok := s.MetricMap[name]
	if !ok {
		return nil
	}

	if actualType := family.GetType(); actualType != metricType {
		s.t.Errorf("Expected %s to be of type %s but was %s",
			name, dto.MetricType_name[int32(metricType)], dto.MetricType_name[int32(actualType)])
		return nil
	}

	var metric *dto.Metric
Outer:
	for _, m := range family.GetMetric() {
		labelPairs := m.GetLabel()
		if len(labelPairs) != len(labels) {
			continue
		}
		for _, labelPair := range labelPairs {
			if labelValue, ok := labels[labelPair.GetName()]; !ok || labelValue != labelPair.GetValue() {
				continue Outer
			}
		}
		metric = m
		break
	}

	return metric
}

func floatEquals(a, b float64) bool {
	epsilon := 0.00000001
	return math.Abs(a-b) < epsilon
}
