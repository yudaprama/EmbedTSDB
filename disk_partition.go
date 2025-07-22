package embedtsdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yudaprama/embedtsdb/internal/syscall"
)

const (
	dataFileName = "data"
	metaFileName = "meta.json"
)

var (
	errInvalidPartition = errors.New("invalid partition")
)

// A disk partition implements a partition that uses local disk as a storage.
// It mainly has two files, data file and meta file.
// The data file is memory-mapped and read only; no need to lock at all.
type diskPartition struct {
	dirPath string
	meta    meta
	// file descriptor of data file
	f *os.File
	// memory-mapped file backed by f
	mappedFile []byte
	// duration to store data
	retention time.Duration
}

// meta is a mapper for a meta file, which is put for each partition.
// Note that the CreatedAt is surely timestamped by embedtsdb but Min/Max Timestamps are likely to do by other process.
type meta struct {
	MinTimestamp  int64                 `json:"minTimestamp"`
	MaxTimestamp  int64                 `json:"maxTimestamp"`
	NumDataPoints int                   `json:"numDataPoints"`
	Metrics       map[string]diskMetric `json:"metrics"`
	CreatedAt     time.Time             `json:"createdAt"`
}

// diskMetric holds meta data to access actual data from the memory-mapped file.
type diskMetric struct {
	Name          string `json:"name"`
	Offset        int64  `json:"offset"`
	MinTimestamp  int64  `json:"minTimestamp"`
	MaxTimestamp  int64  `json:"maxTimestamp"`
	NumDataPoints int64  `json:"numDataPoints"`
}

// openDiskPartition first maps the data file into memory with memory-mapping.
func openDiskPartition(dirPath string, retention time.Duration) (partition, error) {
	if dirPath == "" {
		return nil, fmt.Errorf("dir path is required")
	}
	metaFilePath := filepath.Join(dirPath, metaFileName)
	_, err := os.Stat(metaFilePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, errInvalidPartition
	}

	// Map data to the memory
	dataPath := filepath.Join(dirPath, dataFileName)
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read data file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close() // Close file on error
		return nil, fmt.Errorf("failed to fetch file info: %w", err)
	}
	if info.Size() == 0 {
		f.Close() // Close file on error
		return nil, ErrNoDataPoints
	}
	mapped, err := syscall.Mmap(int(f.Fd()), int(info.Size()))
	if err != nil {
		f.Close() // Close file on error
		return nil, fmt.Errorf("failed to perform mmap: %w", err)
	}

	// Read metadata to the heap
	m := meta{}
	mf, err := os.Open(metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	defer mf.Close()
	decoder := json.NewDecoder(mf)
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("failed to decode metadata: %w", err)
	}
	return &diskPartition{
		dirPath:    dirPath,
		meta:       m,
		f:          f,
		mappedFile: mapped,
		retention:  retention,
	}, nil
}

func (d *diskPartition) insertRows(_ []Row) ([]Row, error) {
	return nil, fmt.Errorf("can't insert rows into disk partition")
}

func (d *diskPartition) selectDataPoints(metric string, labels []Label, start, end int64) ([]*DataPoint, error) {
	if d.expired() {
		return nil, fmt.Errorf("this partition is expired: %w", ErrNoDataPoints)
	}
	name := marshalMetricName(metric, labels)
	mt, ok := d.meta.Metrics[name]
	if !ok {
		return nil, ErrNoDataPoints
	}
	r := bytes.NewReader(d.mappedFile)
	if _, err := r.Seek(mt.Offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}
	decoder, err := newSeriesDecoder(r)
	if err != nil {
		return nil, fmt.Errorf("failed to generate decoder for metric %q in %q: %w", name, d.dirPath, err)
	}
	defer putSeriesDecoder(decoder)

	// TODO: Divide fixed-lengh chunks when flushing, and index it.
	points := dataPointSlicePool.Get().([]*DataPoint)
	points = points[:0] // Reset length but keep capacity
	defer dataPointSlicePool.Put(points)
	for i := 0; i < int(mt.NumDataPoints); i++ {
		point := dataPointPool.Get().(*DataPoint)
		if err := decoder.decodePoint(point); err != nil {
			dataPointPool.Put(point)
			return nil, fmt.Errorf("failed to decode point of metric %q in %q: %w", name, d.dirPath, err)
		}
		if point.Timestamp < start {
			dataPointPool.Put(point)
			continue
		}
		if point.Timestamp >= end {
			dataPointPool.Put(point)
			break
		}
		points = append(points, point)
	}
	// Create a new slice and copy DataPoint values (not pointers)
	result := make([]*DataPoint, len(points))
	for i, p := range points {
		newPoint := &DataPoint{
			Timestamp: p.Timestamp,
			Value:     p.Value,
		}
		result[i] = newPoint
		dataPointPool.Put(p) // Return pooled DataPoint
	}
	return result, nil
}

func (d *diskPartition) minTimestamp() int64 {
	return d.meta.MinTimestamp
}

func (d *diskPartition) maxTimestamp() int64 {
	return d.meta.MaxTimestamp
}

func (d *diskPartition) size() int {
	return d.meta.NumDataPoints
}

// Disk partition is immutable.
func (d *diskPartition) active() bool {
	return false
}

func (d *diskPartition) clean() error {
	// Unmap memory first
	if d.mappedFile != nil {
		if err := syscall.Munmap(d.mappedFile); err != nil {
			return fmt.Errorf("failed to unmap memory: %w", err)
		}
		d.mappedFile = nil
	}

	// Close file descriptor
	if d.f != nil {
		if err := d.f.Close(); err != nil {
			return fmt.Errorf("failed to close file descriptor: %w", err)
		}
		d.f = nil
	}

	// Remove files
	if err := os.RemoveAll(d.dirPath); err != nil {
		return fmt.Errorf("failed to remove all files inside the partition (%d~%d): %w", d.minTimestamp(), d.maxTimestamp(), err)
	}

	return nil
}

func (d *diskPartition) expired() bool {
	diff := time.Since(d.meta.CreatedAt)
	if diff > d.retention {
		return true
	}
	return false
}
