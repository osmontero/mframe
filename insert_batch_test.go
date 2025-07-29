package mframe_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/threatwinds/mframe"
)

func TestInsertBatch(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Test empty batch
	err := df.InsertBatch([]map[mframe.KeyName]interface{}{})
	if err == nil {
		t.Error("Expected error for empty batch")
	}

	// Test valid batch
	batch := []map[mframe.KeyName]interface{}{
		{"name": "test1", "value": 100.0, "active": true},
		{"name": "test2", "value": 200.0, "active": false},
		{"name": "test3", "value": 300.0, "active": true},
	}

	err = df.InsertBatch(batch)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if df.Count() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Count())
	}

	// Test batch with nil and empty entries
	batch2 := []map[mframe.KeyName]interface{}{
		{"name": "test4", "value": 400.0},
		nil,
		{},
		{"name": "test5", "value": 500.0},
	}

	err = df.InsertBatch(batch2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if df.Count() != 5 {
		t.Errorf("Expected 5 rows, got %d", df.Count())
	}
}

func TestInsertBatchWithIDs(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Test empty batch
	err := df.InsertBatchWithIDs(map[uuid.UUID]map[mframe.KeyName]interface{}{})
	if err == nil {
		t.Error("Expected error for empty batch")
	}

	// Test valid batch with IDs
	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()

	entries := map[uuid.UUID]map[mframe.KeyName]interface{}{
		id1: {"name": "test1", "value": 100.0},
		id2: {"name": "test2", "value": 200.0},
		id3: {"name": "test3", "value": 300.0},
	}

	err = df.InsertBatchWithIDs(entries)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if df.Count() != 3 {
		t.Errorf("Expected 3 rows, got %d", df.Count())
	}

	// Verify specific IDs exist
	result := df.Filter(mframe.Equals, "name", "test1", nil)
	if result.Count() != 1 {
		t.Error("Expected to find test1")
	}

	// Test batch with nil and empty entries
	id4 := uuid.New()
	id5 := uuid.New()
	id6 := uuid.New()

	entries2 := map[uuid.UUID]map[mframe.KeyName]interface{}{
		id4: {"name": "test4", "value": 400.0},
		id5: nil,
		id6: {},
	}

	err = df.InsertBatchWithIDs(entries2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if df.Count() != 4 {
		t.Errorf("Expected 4 rows, got %d", df.Count())
	}
}

func TestInsertWithError(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Test nil data
	err := df.InsertWithError(nil)
	if err == nil {
		t.Error("Expected error for nil data")
	}

	// Test empty data
	err = df.InsertWithError(map[mframe.KeyName]interface{}{})
	if err == nil {
		t.Error("Expected error for empty data")
	}

	// Test valid data
	err = df.InsertWithError(map[mframe.KeyName]interface{}{
		"name":  "test",
		"value": 123.45,
	})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if df.Count() != 1 {
		t.Errorf("Expected 1 row, got %d", df.Count())
	}
}

func TestInitWithOptions(t *testing.T) {
	df := &mframe.DataFrame{}
	df.InitWithOptions(5*time.Minute, 500)

	// The regex cache size is internal, so we can't directly test it
	// But we can verify the DataFrame is initialized properly
	if df.TTL != 5*time.Minute {
		t.Errorf("Expected TTL to be 5 minutes, got %v", df.TTL)
	}

	// Test with zero cache size (should use default)
	df2 := &mframe.DataFrame{}
	df2.InitWithOptions(10*time.Minute, 0)

	if df2.TTL != 10*time.Minute {
		t.Errorf("Expected TTL to be 10 minutes, got %v", df2.TTL)
	}
}

func TestClearRegexCache(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert data and use regex filters to populate cache
	for i := 0; i < 100; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"email": "user@example.com",
			"name":  "test",
		})
	}

	// Use regex filter to populate cache
	df.Filter(mframe.RegExp, "email", "user@.*", nil)
	df.Filter(mframe.RegExp, "name", "test.*", nil)

	// Clear the cache
	df.ClearRegexCache()

	// The cache should be empty now, but we can't directly verify
	// We can only verify the method doesn't panic
}

func TestNumericTypes(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Test all numeric types
	df.Insert(map[mframe.KeyName]interface{}{
		"int8":    int8(127),
		"int16":   int16(32767),
		"int32":   int32(2147483647),
		"int64":   int64(9223372036854775807),
		"uint8":   uint8(255),
		"uint16":  uint16(65535),
		"uint32":  uint32(4294967295),
		"uint64":  uint64(18446744073709551615),
		"float32": float32(3.14),
		"float64": float64(3.14159),
	})

	if df.Count() != 1 {
		t.Errorf("Expected 1 row, got %d", df.Count())
	}

	// Verify all fields were indexed as numeric
	result := df.Filter(mframe.Equals, "int8", 127.0, nil)
	if result.Count() != 1 {
		t.Error("int8 not properly indexed")
	}

	result = df.Filter(mframe.Equals, "uint8", 255.0, nil)
	if result.Count() != 1 {
		t.Error("uint8 not properly indexed")
	}

	result = df.Filter(mframe.Equals, "float32", float64(float32(3.14)), nil)
	if result.Count() != 1 {
		t.Error("float32 not properly indexed")
	}
}
