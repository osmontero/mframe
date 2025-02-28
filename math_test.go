package mframe_test

import (
	"testing"
	"time"

	"github.com/threatwinds/mframe"
)

func TestCount(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := map[mframe.KeyName]string{
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
			map[mframe.KeyName]interface{}{
				k: v,
			},
		)
	}

	result := cache.Count()

	if result != len(kvs) {
		t.Errorf("Expected count %d, but got %v", len(kvs), result)
	}
}

func TestCountUnique(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "name": "John"},
		{"id": 2, "name": "Jane"},
		{"id": 3, "name": "John"},
		{"id": 4, "name": "Jane"},
		{"id": 5, "name": "John"},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result := cache.CountUnique("name")

	expectedCounts := map[interface{}]int{
		"John": 3,
		"Jane": 2,
	}

	for value, count := range result {
		if c, ok := expectedCounts[value]; ok && c != count {
			t.Errorf("Expected count %v for value %s, but got %v", expectedCounts[value], value, count)
		} else if !ok {
			t.Errorf("Got an unexpected value %v", value)
		}
	}
}

func TestSum(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result, err := cache.Sum("value")
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if result != 15.0 {
		t.Errorf("Expected sum 15.0, but got %v", result)
	}
}

func TestAverage(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result, err := cache.Average("value")
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if result != 3.0 {
		t.Errorf("Expected average 3.0, but got %v", result)
	}
}

func TestMedian(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result, err := cache.Median("value")
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if result != 3.0 {
		t.Errorf("Expected median 3.0, but got %v", result)
	}
}

func TestMax(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result, err := cache.Max("value")
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if result != 5.0 {
		t.Errorf("Expected max 5.0, but got %v", result)
	}
}

func TestMin(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result, err := cache.Min("value")
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if result != 1.0 {
		t.Errorf("Expected min 1.0, but got %v", result)
	}
}

func TestVariance(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "value": 1.0},
		{"id": 2, "value": 2.0},
		{"id": 3, "value": 3.0},
		{"id": 4, "value": 4.0},
		{"id": 5, "value": 5.0},
		{"id": 5, "values": 5.0},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	result, err := cache.Variance("value")
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}

	if result != 2.0 {
		t.Errorf("Expected variance 2.0, but got %v", result)
	}
}
