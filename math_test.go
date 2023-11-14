package mframe_test

import (
	"testing"
	"time"

	"github.com/threatwinds/mframe"
)

func TestCount(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := map[string]string{
		"001": "value1",
		"002": "value2",
		"004": "value3",
		"005": "value4",
		"006": "value5",
		"007": "value6",
		"008": "value7",
		"009": "value8",
		"010": "value9",
		"011": "value10",
		"012": "value11",
	}

	for k, v := range kvs {
		cache.Insert(
			map[string]interface{}{
				k: v,
			},
		)
	}

	result := cache.Count("count")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["count"] != float64(len(kvs)) {
			t.Errorf("Expected count %d, but got %v", len(kvs), row["count"])
		}
	}
}

func TestCountUnique(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "name": "John"},
		{"id": 2, "name": "Jane"},
		{"id": 3, "name": "John"},
		{"id": 4, "name": "Jane"},
		{"id": 5, "name": "John"},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.CountUnique("count", "name")

	if len(result.Data) != 2 {
		t.Errorf("Expected 2 rows, but got %d", len(result.Data))
	}

	expectedCounts := map[string]float64{
		"John": 3,
		"Jane": 2,
	}

	for _, row := range result.Data {
		name, ok := row["value"]
		if !ok {
			continue
		}
		count := row["count"]

		if expectedCounts[name.(string)] != count {
			t.Errorf("Expected count %v for name %s, but got %v", expectedCounts[name.(string)], name, count)
		}
	}
}

func TestSum(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.Sum("sum", "value")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["sum"] != 15.0 {
			t.Errorf("Expected sum 15.0, but got %v", row["sum"])
		}
	}
}

func TestAverage(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.Average("average", "value")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["average"] != 3.0 {
			t.Errorf("Expected average 3.0, but got %v", row["average"])
		}
	}
}

func TestMedian(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.Median("median", "value")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["median"] != 3.0 {
			t.Errorf("Expected median 3.0, but got %v", row["median"])
		}
	}
}

func TestMax(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.Max("max", "value")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["max"] != 5.0 {
			t.Errorf("Expected max 5.0, but got %v", row["max"])
		}
	}
}

func TestMin(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.Min("min", "value")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["min"] != 1.0 {
			t.Errorf("Expected min 1.0, but got %v", row["min"])
		}
	}
}

func TestVariance(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[string]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.Variance("variance", "value")

	if len(result.Data) != 1 {
		t.Errorf("Expected 1 row, but got %d", len(result.Data))
	}

	for _, row := range result.Data {
		if row["variance"] != 2.0 {
			t.Errorf("Expected variance 2.0, but got %v", row["variance"])
		}
	}
}
