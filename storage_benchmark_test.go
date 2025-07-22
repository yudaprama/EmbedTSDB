package embedtsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkStorage_InsertRows(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

// Select data points among a thousand data in memory
func BenchmarkStorage_SelectAmongThousandPoints(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	for i := 1; i < 1000; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		_, _ = storage.Select("metric1", nil, 10, 100)
	}
}

// Select data points among a million data in memory
func BenchmarkStorage_SelectAmongMillionPoints(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	for i := 1; i < 1000000; i++ {
		storage.InsertRows([]Row{
			{Metric: "metric1", DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
	b.ResetTimer()
	for i := 1; i < b.N; i++ {
		_, _ = storage.Select("metric1", nil, 10, 100)
	}
}

// Benchmark concurrent insertions to test the optimized concurrency patterns
func BenchmarkStorage_ConcurrentInsertRows(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			storage.InsertRows([]Row{
				{Metric: "metric1", DataPoint: DataPoint{Timestamp: i, Value: 0.1}},
			})
			i++
		}
	})
}

// Benchmark memory allocation patterns with labeled metrics
func BenchmarkStorage_InsertRowsWithLabels(b *testing.B) {
	storage, err := NewStorage()
	require.NoError(b, err)
	labels := []Label{
		{Name: "host", Value: "host-1"},
		{Name: "region", Value: "us-west-1"},
		{Name: "service", Value: "api"},
	}
	b.ResetTimer()

	for i := 1; i < b.N; i++ {
		storage.InsertRows([]Row{
			{Metric: "cpu_usage", Labels: labels, DataPoint: DataPoint{Timestamp: int64(i), Value: 0.1}},
		})
	}
}

// Benchmark marshal metric name optimizations
func BenchmarkMarshalMetricName(b *testing.B) {
	labels := []Label{
		{Name: "host", Value: "host-1"},
		{Name: "region", Value: "us-west-1"},
		{Name: "service", Value: "api"},
		{Name: "environment", Value: "production"},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marshalMetricName("cpu_usage_percent", labels)
	}
}

// Benchmark WAL append operations to test buffer pooling
func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()
	wal, err := newDiskWAL(dir, 4096)
	require.NoError(b, err)
	defer wal.removeAll()

	rows := []Row{
		{Metric: "metric1", DataPoint: DataPoint{Timestamp: 1600000000, Value: 0.1}},
		{Metric: "metric2", DataPoint: DataPoint{Timestamp: 1600000001, Value: 0.2}},
		{Metric: "metric3", DataPoint: DataPoint{Timestamp: 1600000002, Value: 0.3}},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wal.append(operationInsert, rows)
	}
}
