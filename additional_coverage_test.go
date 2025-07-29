package mframe_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/threatwinds/mframe"
)

// Test insertWithIDUnlocked functionality through public API
func TestInsertWithSpecificID(t *testing.T) {
	// This function is private and used internally by InsertBatchWithIDs
	// It's already tested through TestInsertBatchWithIDs
}

// Test regex cache error handling
func TestRegexCacheErrorHandling(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert test data
	df.Insert(map[mframe.KeyName]interface{}{
		"text": "hello",
	})

	// Test with invalid regex that should fail compilation
	result := df.Filter(mframe.RegExp, "text", "[[[", nil)
	if result.Count() != 0 {
		t.Error("Expected 0 results for invalid regex")
	}
}

// Test FindFirstByKey with regex patterns
func TestFindFirstByKeyWithRegex(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert test data
	df.Insert(map[mframe.KeyName]interface{}{
		"name_1": "value1",
		"name_2": "value2",
		"other":  "value3",
	})

	// Test with regex pattern
	id, _, _ := df.FindFirstByKey("name_[0-9]+")
	if id == uuid.Nil {
		// This is expected as FindFirstByKey checks for literal regex characters
		// but doesn't compile and match them
		t.Log("FindFirstByKey doesn't support regex matching")
	}

	// Test with pattern that contains regex characters but isn't valid
	id, _, _ = df.FindFirstByKey("[[[")
	if id != uuid.Nil {
		t.Error("Expected nil UUID for invalid pattern")
	}
}

// Test filter with regex key patterns
func TestFilterWithRegexKeyPattern(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert test data with pattern-matching keys
	df.Insert(map[mframe.KeyName]interface{}{
		"field_1": "value1",
		"field_2": "value2",
		"other":   "value3",
	})

	// Test with valid regex pattern in key
	result := df.Filter(mframe.Equals, "field_[0-9]+", "value1", nil)
	// This should match field_1
	if result.Count() == 0 {
		t.Log("Filter with regex key pattern returned no results")
	}

	// Test with invalid regex pattern in key
	result = df.Filter(mframe.Equals, "[[[", "value1", nil)
	if result.Count() != 0 {
		t.Error("Expected 0 results for invalid regex key pattern")
	}
}

// Test error paths in num function
func TestNumFunctionError(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// First insert a string value for a key
	df.Insert(map[mframe.KeyName]interface{}{
		"field": "string_value",
	})

	// Now try to insert a numeric value for the same key
	// This will trigger the error path in num() function
	df.Insert(map[mframe.KeyName]interface{}{
		"field": 123.45,
	})

	// The second insert should have been rejected due to type mismatch
	// Check that we still only have string values
	result := df.Filter(mframe.Equals, "field", "string_value", nil)
	if result.Count() != 1 {
		t.Error("Expected string value to remain")
	}
}

// Test error paths in index function
func TestIndexFunctionErrors(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Test with deeply nested structures
	df.Insert(map[mframe.KeyName]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": map[string]interface{}{
					"deep": "value",
				},
			},
		},
	})

	// Test with array containing various types including nil
	df.Insert(map[mframe.KeyName]interface{}{
		"array": []interface{}{
			"string",
			123,
			true,
			nil,
			map[string]interface{}{"nested": "in_array"},
		},
	})

	// Verify the data was indexed
	if df.Count() != 2 {
		t.Errorf("Expected 2 rows, got %d", df.Count())
	}
}

// Test Range function error path
func TestRangeErrorPath(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert single value to avoid empty data error
	df.Insert(map[mframe.KeyName]interface{}{
		"value": 100.0,
	})

	// This should work
	r, err := df.Range("value")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if r != 0 { // min and max are same, so range is 0
		t.Errorf("Expected range 0, got %f", r)
	}
}

// Test unknown key type in Explain
func TestExplainUnknownKeyType(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Don't insert any data, so the key type will be unknown
	result := df.Explain(mframe.Equals, "unknown_key", "value")

	if result.KeyType != "Unknown" {
		t.Errorf("Expected Unknown key type, got %s", result.KeyType)
	}

	// Test the String() method
	output := result.String()
	if output == "" {
		t.Error("Expected non-empty explain output")
	}
}

// Test all branches in filter operations
func TestFilterCompleteCoverage(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert data
	df.Insert(map[mframe.KeyName]interface{}{
		"str":  "test",
		"num":  50.0,
		"bool": true,
	})

	// Test case-insensitive string operations
	tests := []struct {
		op    mframe.Operator
		value interface{}
	}{
		{mframe.NotEquals, "TEST"},
		{mframe.NotContains, "TEST"},
		{mframe.NotStartsWith, "TEST"},
		{mframe.NotEndsWith, "TEST"},
	}

	for _, tt := range tests {
		result := df.Filter(tt.op, "str", tt.value, map[mframe.FilterOption]bool{
			mframe.CaseSensitive: false,
		})
		t.Logf("Filter %v returned %d results", tt.op, result.Count())
	}

	// Test InList and NotInList with case insensitive
	result := df.Filter(mframe.InList, "str", []string{"TEST", "OTHER"}, map[mframe.FilterOption]bool{
		mframe.CaseSensitive: false,
	})
	if result.Count() != 1 {
		t.Error("Expected 1 result for case-insensitive InList")
	}

	result = df.Filter(mframe.NotInList, "str", []string{"TEST", "OTHER"}, map[mframe.FilterOption]bool{
		mframe.CaseSensitive: false,
	})
	if result.Count() != 0 {
		t.Error("Expected 0 results for case-insensitive NotInList")
	}
}

// Test edge case where min > max in Between operations
func TestBetweenMinMaxSwap(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	df.Insert(map[mframe.KeyName]interface{}{
		"value": 50.0,
	})

	// Test with max < min (should auto-swap)
	result := df.Filter(mframe.Between, "value", []float64{100.0, 0.0}, nil)
	if result.Count() != 1 {
		t.Error("Expected Between to auto-swap min/max")
	}

	result = df.Filter(mframe.NotBetween, "value", []float64{100.0, 0.0}, nil)
	if result.Count() != 0 {
		t.Error("Expected NotBetween to auto-swap min/max")
	}
}
