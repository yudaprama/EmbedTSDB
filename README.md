# EmbedTSDB

[![Go Version](https://img.shields.io/badge/Go-1.24.4+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Test Coverage](https://codecov.io/gh/yudaprama/embedtsdb/branch/main/graph/badge.svg)](https://codecov.io/gh/yudaprama/embedtsdb)

EmbedTSDB is a lightweight, embedded time series database written in Go that provides goroutine-safe capabilities for insertion and retrieval of time-series data. It can operate both as an in-memory database and as a persistent on-disk storage solution.

## 🚀 Features

- **🔒 Thread-Safe**: Goroutine-safe read and write operations
- **💾 Dual Storage**: Supports both in-memory and persistent disk storage
- **⚡ High Performance**: Optimized for concurrent operations with object pooling
- **🕒 Flexible Timestamps**: Configurable precision (nanoseconds, microseconds, milliseconds, seconds)
- **📊 Time-Based Partitioning**: Automatic data partitioning based on time ranges
- **🔄 WAL Support**: Write-Ahead Logging for data durability
- **♻️ Data Retention**: Automatic cleanup of old data based on retention policies
- **🏷️ Labels Support**: Multi-dimensional data organization with labels
- **📈 Memory Efficient**: Object pooling and optimized memory usage
- **⚙️ Configurable**: Extensive configuration options for various use cases

## 📦 Installation

```bash
go get github.com/yudaprama/embedtsdb
```

## 🔧 Quick Start

### Basic In-Memory Usage

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/yudaprama/embedtsdb"
)

func main() {
    // Create an in-memory storage
    storage, err := embedtsdb.NewStorage()
    if err != nil {
        log.Fatal(err)
    }
    defer storage.Close()

    // Insert some data points
    err = storage.InsertRows([]embedtsdb.Row{
        {
            Metric: "cpu_usage",
            Labels: []embedtsdb.Label{
                {Name: "host", Value: "server-1"},
                {Name: "region", Value: "us-east-1"},
            },
            DataPoint: embedtsdb.DataPoint{
                Timestamp: time.Now().Unix(),
                Value:     85.5,
            },
        },
        {
            Metric: "memory_usage",
            Labels: []embedtsdb.Label{
                {Name: "host", Value: "server-1"},
            },
            DataPoint: embedtsdb.DataPoint{
                Timestamp: time.Now().Unix(),
                Value:     72.3,
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Query data
    points, err := storage.Select("cpu_usage", 
        []embedtsdb.Label{{Name: "host", Value: "server-1"}},
        time.Now().Unix()-3600, // last hour
        time.Now().Unix(),
    )
    if err != nil {
        log.Fatal(err)
    }

    for _, point := range points {
        fmt.Printf("Timestamp: %d, Value: %f\n", point.Timestamp, point.Value)
    }
}
```

### Persistent Storage

```go
// Create persistent storage
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithDataPath("./data"),
    embedtsdb.WithPartitionDuration(1*time.Hour),
    embedtsdb.WithRetention(24*time.Hour),
    embedtsdb.WithTimestampPrecision(embedtsdb.Seconds),
)
```

## 📚 API Documentation

### Core Types

#### `Storage` Interface

The main interface for interacting with the time series database:

```go
type Storage interface {
    Reader
    InsertRows(rows []Row) error
    Close() error
}

type Reader interface {
    Select(metric string, labels []Label, start, end int64) ([]*DataPoint, error)
}
```

#### `Row` Structure

Represents a complete data entry:

```go
type Row struct {
    Metric    string     // The unique name of metric (required)
    Labels    []Label    // Optional key-value properties for identification
    DataPoint DataPoint  // The actual data point (required)
}
```

#### `DataPoint` Structure

The smallest unit of time series data:

```go
type DataPoint struct {
    Value     float64  // The actual value (required)
    Timestamp int64    // Unix timestamp
}
```

#### `Label` Structure

Key-value pairs for multi-dimensional data organization:

```go
type Label struct {
    Name  string  // Label name (max 256 characters)
    Value string  // Label value (max 16KB)
}
```

### Configuration Options

#### `WithDataPath(path string)`
Specifies the directory path for persistent storage.

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithDataPath("./timeseries-data"),
)
```

#### `WithPartitionDuration(duration time.Duration)`
Sets the time range for data partitions (default: 1 hour).

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithPartitionDuration(30 * time.Minute),
)
```

#### `WithRetention(retention time.Duration)`
Configures automatic data cleanup after the specified duration (default: 14 days).

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithRetention(7 * 24 * time.Hour), // 7 days
)
```

#### `WithTimestampPrecision(precision TimestampPrecision)`
Sets timestamp precision for all operations.

Available precisions:
- `embedtsdb.Nanoseconds` (default)
- `embedtsdb.Microseconds`
- `embedtsdb.Milliseconds`
- `embedtsdb.Seconds`

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithTimestampPrecision(embedtsdb.Milliseconds),
)
```

#### `WithWriteTimeout(timeout time.Duration)`
Sets timeout for write operations when workers are busy (default: 30 seconds).

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithWriteTimeout(60 * time.Second),
)
```

#### `WithWALBufferedSize(size int)`
Configures Write-Ahead Logging buffer size (default: 4096 bytes).

- `size > 0`: Buffered writes with specified buffer size
- `size = 0`: Immediate writes (no buffering)
- `size = -1`: Disable WAL

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithWALBufferedSize(8192), // 8KB buffer
)
```

#### `WithLogger(logger Logger)`
Sets a custom logger for verbose output.

```go
storage, err := embedtsdb.NewStorage(
    embedtsdb.WithLogger(myLogger),
)
```

## 💡 Usage Examples

### Concurrent Operations

```go
package main

import (
    "sync"
    "time"
    "github.com/yudaprama/embedtsdb"
)

func main() {
    storage, err := embedtsdb.NewStorage(
        embedtsdb.WithPartitionDuration(5*time.Hour),
        embedtsdb.WithTimestampPrecision(embedtsdb.Seconds),
    )
    if err != nil {
        panic(err)
    }
    defer storage.Close()

    var wg sync.WaitGroup

    // Concurrent writes
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            timestamp := time.Now().Unix() + int64(id)
            storage.InsertRows([]embedtsdb.Row{
                {
                    Metric: "concurrent_metric",
                    Labels: []embedtsdb.Label{
                        {Name: "worker_id", Value: fmt.Sprintf("%d", id)},
                    },
                    DataPoint: embedtsdb.DataPoint{
                        Timestamp: timestamp,
                        Value:     float64(id),
                    },
                },
            })
        }(i)
    }

    // Concurrent reads
    for i := 0; i < 50; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            now := time.Now().Unix()
            points, _ := storage.Select("concurrent_metric", nil, now-100, now+100)
            fmt.Printf("Found %d points\n", len(points))
        }()
    }

    wg.Wait()
}
```

### Metrics with Multiple Labels

```go
// Insert metrics with different label combinations
err = storage.InsertRows([]embedtsdb.Row{
    {
        Metric: "http_requests",
        Labels: []embedtsdb.Label{
            {Name: "method", Value: "GET"},
            {Name: "status", Value: "200"},
            {Name: "endpoint", Value: "/api/users"},
        },
        DataPoint: embedtsdb.DataPoint{
            Timestamp: time.Now().Unix(),
            Value:     150,
        },
    },
    {
        Metric: "http_requests",
        Labels: []embedtsdb.Label{
            {Name: "method", Value: "POST"},
            {Name: "status", Value: "201"},
            {Name: "endpoint", Value: "/api/users"},
        },
        DataPoint: embedtsdb.DataPoint{
            Timestamp: time.Now().Unix(),
            Value:     25,
        },
    },
})

// Query specific label combinations
getRequests, err := storage.Select("http_requests",
    []embedtsdb.Label{
        {Name: "method", Value: "GET"},
        {Name: "status", Value: "200"},
    },
    start, end,
)
```

## 🔧 Development

### Running Tests

```bash
# Run all tests with race detection and coverage
make test

# Run benchmarks
make test-bench

# View memory profiling
make pprof-mem

# View CPU profiling
make pprof-cpu
```

### Project Structure

```
EmbedTSDB/
├── storage.go              # Main storage implementation
├── partition.go            # Partition interface
├── memory_partition.go     # In-memory partition implementation
├── disk_partition.go       # Disk-based partition implementation
├── partition_list.go       # Partition management
├── wal.go                  # Write-Ahead Logging
├── disk_wal.go            # Disk-based WAL implementation
├── encoding.go            # Data encoding utilities
├── label.go               # Label handling
├── internal/
│   ├── cgroup/            # CPU and memory resource detection
│   ├── encoding/          # Internal encoding utilities
│   ├── syscall/           # System call wrappers
│   └── timerpool/         # Timer pool for performance
└── testdata/              # Test data files
```

## 📊 Performance

EmbedTSDB is designed for high-performance time series workloads:

- **Concurrent Workers**: Automatically limits concurrent operations based on available CPU cores
- **Memory Optimization**: Uses object pools to reduce garbage collection pressure
- **Efficient Encoding**: Optimized binary encoding for storage efficiency
- **Partition-Based**: Time-based partitioning for efficient queries and data management

### Benchmarks

Run benchmarks to see performance characteristics:

```bash
go test -bench=. -benchmem
```

Example benchmark results show the system can handle thousands of operations per second with minimal memory allocation.

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Running Tests Locally

```bash
# Install dependencies
make dep

# Run tests
make test

# Run benchmarks
make test-bench

# Start godoc server for documentation
make godoc
```

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- [Prometheus](https://prometheus.io/) - Monitoring and alerting toolkit
- [InfluxDB](https://www.influxdata.com/) - Time series database platform
- [TimescaleDB](https://www.timescale.com/) - PostgreSQL-based time series database

## 📧 Support

If you have questions or need help, please:

1. Check the [documentation](#-api-documentation)
2. Search existing [issues](https://github.com/yudaprama/embedtsdb/issues)
3. Create a new issue if needed

---

**EmbedTSDB** - Lightweight embedded time series database for Go applications. 