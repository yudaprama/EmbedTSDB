package embedtsdb

import (
	"sort"
	"sync"

	"github.com/yudaprama/embedtsdb/internal/encoding"
)

// Buffer pool for metric name marshaling
var metricNameBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 512) // Pre-allocate capacity for typical metric names
	},
}

const (
	// The maximum length of label name.
	//
	// Longer names are truncated.
	maxLabelNameLen = 256

	// The maximum length of label value.
	//
	// Longer values are truncated.
	maxLabelValueLen = 16 * 1024
)

// Label is a time-series label.
// A label with missing name or value is invalid.
type Label struct {
	Name  string
	Value string
}

// marshalMetricName builds a unique bytes by encoding labels.
func marshalMetricName(metric string, labels []Label) string {
	if len(labels) == 0 {
		return metric
	}
	invalid := func(name, value string) bool {
		return name == "" || value == ""
	}

	// Determine the bytes size in advance.
	size := len(metric) + 2
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})
	for i := range labels {
		label := &labels[i]
		if invalid(label.Name, label.Value) {
			continue
		}
		if len(label.Name) > maxLabelNameLen {
			label.Name = label.Name[:maxLabelNameLen]
		}
		if len(label.Value) > maxLabelValueLen {
			label.Value = label.Value[:maxLabelValueLen]
		}
		size += len(label.Name)
		size += len(label.Value)
		size += 4
	}

	// Start building the bytes using pooled buffer.
	out := metricNameBufferPool.Get().([]byte)
	out = out[:0] // Reset length but keep capacity
	defer metricNameBufferPool.Put(out)

	out = encoding.MarshalUint16(out, uint16(len(metric)))
	out = append(out, metric...)
	for i := range labels {
		label := &labels[i]
		if invalid(label.Name, label.Value) {
			continue
		}
		out = encoding.MarshalUint16(out, uint16(len(label.Name)))
		out = append(out, label.Name...)
		out = encoding.MarshalUint16(out, uint16(len(label.Value)))
		out = append(out, label.Value...)
	}
	// Copy bytes to string to avoid referencing pooled buffer after return
	return string(append([]byte(nil), out...))
}
