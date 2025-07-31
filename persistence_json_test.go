package mframe_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/threatwinds/mframe"
)

func TestDataFrame_ExportImportJSON(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-json-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create and populate a DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	// Insert various types of data
	now := time.Now().UTC().Truncate(time.Second)
	testUUID := uuid.New()

	testData := []map[mframe.KeyName]interface{}{
		{
			"name":    "Alice",
			"age":     float64(30),
			"active":  true,
			"created": now,
			"id":      testUUID,
		},
		{
			"name":    "Bob",
			"age":     float64(25),
			"active":  false,
			"created": now.Add(-24 * time.Hour),
		},
		{
			"name":    "Charlie",
			"age":     float64(35),
			"active":  true,
			"created": now.Add(-48 * time.Hour),
			"nested":  map[string]interface{}{"city": "New York", "zip": float64(10001)},
		},
	}

	for _, data := range testData {
		df.Insert(data)
	}

	// Export to JSON
	filename := filepath.Join(tempDir, "test.json")
	if err := df.ExportToJSON(filename); err != nil {
		t.Fatalf("Failed to export to JSON: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatal("Exported JSON file does not exist")
	}

	// Create a new DataFrame and import from JSON
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)

	if err := df2.ImportFromJSON(filename); err != nil {
		t.Fatalf("Failed to import from JSON: %v", err)
	}

	// Verify the imported data
	if len(df2.Data) != len(df.Data) {
		t.Errorf("Data length mismatch: got %d, want %d", len(df2.Data), len(df.Data))
	}

	// Verify keys are preserved
	if len(df2.Keys) != len(df.Keys) {
		t.Errorf("Keys length mismatch: got %d, want %d", len(df2.Keys), len(df.Keys))
	}

	// Verify TTL is preserved
	if df2.TTL != df.TTL {
		t.Errorf("TTL mismatch: got %v, want %v", df2.TTL, df.TTL)
	}

	// Test that filtering still works after import
	results := df2.Filter(mframe.Equals, "name", "Alice", nil)
	if len(results.Data) != 1 {
		t.Errorf("Filter after import failed: got %d results, want 1", len(results.Data))
	}

	// Verify time values are preserved correctly
	timeResults := df2.Filter(mframe.Between, "created", []time.Time{
		now.Add(-25 * time.Hour),
		now.Add(-23 * time.Hour),
	}, nil)
	if len(timeResults.Data) != 1 {
		t.Errorf("Time filter after import failed: got %d results, want 1", len(timeResults.Data))
	}

	// Verify nested data is preserved
	nestedResults := df2.Filter(mframe.Equals, "nested.city", "New York", nil)
	if len(nestedResults.Data) != 1 {
		t.Errorf("Nested filter after import failed: got %d results, want 1", len(nestedResults.Data))
	}
}

func TestDataFrame_JSONWithExpiredEntries(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-json-expire-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create DataFrame with very short TTL
	df := &mframe.DataFrame{}
	df.Init(100 * time.Millisecond)

	// Insert data
	df.Insert(map[mframe.KeyName]interface{}{
		"test": "should_expire",
	})

	// Wait for data to expire
	time.Sleep(200 * time.Millisecond)

	// Export (should include expired data)
	filename := filepath.Join(tempDir, "expired.json")
	if err := df.ExportToJSON(filename); err != nil {
		t.Fatalf("Failed to export to JSON: %v", err)
	}

	// Import into new DataFrame with longer TTL
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)

	if err := df2.ImportFromJSON(filename); err != nil {
		t.Fatalf("Failed to import from JSON: %v", err)
	}

	// Data should be imported with expired timestamp
	if len(df2.Data) != 1 {
		t.Errorf("Expected 1 row after import, got %d", len(df2.Data))
	}
}

func TestDataFrame_ImportInvalidJSON(t *testing.T) {
	// Create a temporary file with invalid JSON
	tempFile, err := os.CreateTemp("", "mframe-invalid-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()

	// Write invalid JSON
	_, _ = tempFile.Write([]byte("{invalid json"))
	_ = tempFile.Close()

	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	err = df.ImportFromJSON(tempFile.Name())
	if err == nil {
		t.Error("Expected error when importing invalid JSON")
	}
}

func TestDataFrame_ImportVersionMismatchJSON(t *testing.T) {
	// Create a temporary file with higher version
	tempFile, err := os.CreateTemp("", "mframe-version-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()

	// Write JSON with higher version
	jsonData := `{
		"version": 999,
		"data": {},
		"keys": {},
		"expire_at": {},
		"ttl": "1h0m0s"
	}`
	_, _ = tempFile.Write([]byte(jsonData))
	_ = tempFile.Close()

	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	err = df.ImportFromJSON(tempFile.Name())
	if err == nil {
		t.Error("Expected error for version mismatch")
	}
}

func TestDataFrame_ImportInvalidTTLJSON(t *testing.T) {
	// Create a temporary file with invalid TTL
	tempFile, err := os.CreateTemp("", "mframe-ttl-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()

	// Write JSON with invalid TTL
	jsonData := `{
		"version": 1,
		"data": {},
		"keys": {},
		"expire_at": {},
		"ttl": "invalid-duration"
	}`
	_, _ = tempFile.Write([]byte(jsonData))
	_ = tempFile.Close()

	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	err = df.ImportFromJSON(tempFile.Name())
	if err == nil {
		t.Error("Expected error for invalid TTL")
	}
}

func TestDataFrame_JSONRoundTripAllTypes(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-json-alltype-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	// Insert data with all supported types
	now := time.Now().UTC().Truncate(time.Second)
	testData := map[mframe.KeyName]interface{}{
		"string_val":  "test string",
		"int_val":     int(42),
		"int8_val":    int8(8),
		"int16_val":   int16(16),
		"int32_val":   int32(32),
		"int64_val":   int64(64),
		"uint_val":    uint(42),
		"uint8_val":   uint8(8),
		"uint16_val":  uint16(16),
		"uint32_val":  uint32(32),
		"uint64_val":  uint64(64),
		"float32_val": float32(3.14),
		"float64_val": float64(2.718),
		"bool_val":    true,
		"time_val":    now,
		"uuid_val":    uuid.New(),
		"nested": map[string]interface{}{
			"inner": "nested value",
			"num":   float64(100),
		},
	}

	df.Insert(testData)

	// Export and import
	filename := filepath.Join(tempDir, "alltypes.json")
	if err := df.ExportToJSON(filename); err != nil {
		t.Fatalf("Failed to export: %v", err)
	}

	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)
	if err := df2.ImportFromJSON(filename); err != nil {
		t.Fatalf("Failed to import: %v", err)
	}

	// Verify all data types are preserved
	if len(df2.Data) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(df2.Data))
	}

	// Check specific values
	var row mframe.Row
	for _, r := range df2.Data {
		row = r
		break
	}

	// Verify string values
	if v, ok := row["string_val"].(string); !ok || v != "test string" {
		t.Errorf("String value not preserved correctly: %v", row["string_val"])
	}

	// Verify numeric values (all should be float64 after round-trip)
	numericFields := []mframe.KeyName{
		"int_val", "int8_val", "int16_val", "int32_val", "int64_val",
		"uint_val", "uint8_val", "uint16_val", "uint32_val", "uint64_val",
		"float32_val", "float64_val",
	}
	for _, field := range numericFields {
		if _, ok := row[field].(float64); !ok {
			t.Errorf("Numeric field %s not preserved as float64: %T", field, row[field])
		}
	}

	// Verify boolean
	if v, ok := row["bool_val"].(bool); !ok || v != true {
		t.Errorf("Boolean value not preserved correctly: %v", row["bool_val"])
	}

	// Verify time
	if v, ok := row["time_val"].(time.Time); !ok || !v.Equal(now) {
		t.Errorf("Time value not preserved correctly: %v", row["time_val"])
	}

	// Verify UUID (stored as string)
	if v, ok := row["uuid_val"].(string); !ok || v == "" {
		t.Errorf("UUID value not preserved correctly: %v", row["uuid_val"])
	}
}

func BenchmarkDataFrame_ExportToJSON(b *testing.B) {
	// Create temp dir
	tempDir, err := os.MkdirTemp("", "mframe-bench-json-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create and populate DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	for i := 0; i < 1000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":    uuid.New().String(),
			"value": float64(i),
			"name":  "benchmark-" + string(rune('A'+i%26)),
		})
	}

	filename := filepath.Join(tempDir, "bench.json")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := df.ExportToJSON(filename); err != nil {
			b.Fatalf("Failed to export: %v", err)
		}
	}
}

func BenchmarkDataFrame_ImportFromJSON(b *testing.B) {
	// Create temp dir
	tempDir, err := os.MkdirTemp("", "mframe-bench-json-load-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create and save DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	for i := 0; i < 1000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":    uuid.New().String(),
			"value": float64(i),
			"name":  "benchmark-" + string(rune('A'+i%26)),
		})
	}

	filename := filepath.Join(tempDir, "bench.json")
	if err := df.ExportToJSON(filename); err != nil {
		b.Fatalf("Failed to export: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df2 := &mframe.DataFrame{}
		df2.Init(time.Hour)
		if err := df2.ImportFromJSON(filename); err != nil {
			b.Fatalf("Failed to import: %v", err)
		}
	}
}
