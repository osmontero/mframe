package mframe_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/threatwinds/mframe"
)

func TestDataFrame_SaveAndLoad(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create and populate a DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	// Insert various types of data
	testData := []map[mframe.KeyName]interface{}{
		{
			"name":    "Alice",
			"age":     float64(30),
			"active":  true,
			"created": time.Now().UTC(),
		},
		{
			"name":    "Bob",
			"age":     float64(25),
			"active":  false,
			"created": time.Now().UTC().Add(-24 * time.Hour),
		},
		{
			"name":    "Charlie",
			"age":     float64(35),
			"active":  true,
			"created": time.Now().UTC().Add(-48 * time.Hour),
			"nested":  map[string]interface{}{"city": "New York", "zip": float64(10001)},
			"tags":    []interface{}{"developer", "golang"},
		},
	}

	for _, data := range testData {
		df.Insert(data)
	}

	// Test regex cache by performing a regex filter
	df.Filter(mframe.RegExp, "name", "^[AB]", nil)

	// Save to file
	filename := filepath.Join(tempDir, "test.gob")
	if err := df.SaveToFile(filename); err != nil {
		t.Fatalf("Failed to save DataFrame: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Fatal("Saved file does not exist")
	}

	// Create a new DataFrame and load from file
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)

	if err := df2.LoadFromFile(filename); err != nil {
		t.Fatalf("Failed to load DataFrame: %v", err)
	}

	// Verify the loaded data
	if len(df2.Data) != len(df.Data) {
		t.Errorf("Data length mismatch: got %d, want %d", len(df2.Data), len(df.Data))
	}

	// Verify keys are preserved
	if len(df2.Keys) != len(df.Keys) {
		t.Errorf("Keys length mismatch: got %d, want %d", len(df2.Keys), len(df.Keys))
	}

	// Verify string index
	if len(df2.Strings) != len(df.Strings) {
		t.Errorf("Strings index length mismatch: got %d, want %d", len(df2.Strings), len(df.Strings))
	}

	// Verify TTL is preserved
	if df2.TTL != df.TTL {
		t.Errorf("TTL mismatch: got %v, want %v", df2.TTL, df.TTL)
	}

	// Verify version
	if df2.Version != df.Version {
		t.Errorf("Version mismatch: got %d, want %d", df2.Version, df.Version)
	}

	// Test that filtering still works after load
	results := df2.Filter(mframe.Equals, "name", "Alice", nil)
	if len(results.Data) != 1 {
		t.Errorf("Filter after load failed: got %d results, want 1", len(results.Data))
	}
}

func TestDataFrame_SaveAndLoadCompressed(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-test-compressed-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create and populate a DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	// Insert data
	for i := 0; i < 100; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":    uuid.New().String(),
			"value": float64(i),
			"name":  "test-" + string(rune('A'+i%26)),
		})
	}

	// Save compressed
	filename := filepath.Join(tempDir, "test.gob.gz")
	if err := df.SaveToFileCompressed(filename); err != nil {
		t.Fatalf("Failed to save compressed DataFrame: %v", err)
	}

	// Load compressed
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)

	if err := df2.LoadFromFileCompressed(filename); err != nil {
		t.Fatalf("Failed to load compressed DataFrame: %v", err)
	}

	// Verify data
	if len(df2.Data) != len(df.Data) {
		t.Errorf("Data length mismatch: got %d, want %d", len(df2.Data), len(df.Data))
	}

	// Compare file sizes (compressed should be smaller for repetitive data)
	uncompressedFile := filepath.Join(tempDir, "test-uncompressed.gob")
	if err := df.SaveToFile(uncompressedFile); err != nil {
		t.Fatalf("Failed to save uncompressed DataFrame: %v", err)
	}

	compressedInfo, _ := os.Stat(filename)
	uncompressedInfo, _ := os.Stat(uncompressedFile)

	if compressedInfo.Size() >= uncompressedInfo.Size() {
		t.Logf("Warning: Compressed size (%d) is not smaller than uncompressed size (%d)",
			compressedInfo.Size(), uncompressedInfo.Size())
	}
}

func TestDataFrame_LoadNonExistentFile(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	err := df.LoadFromFile("/nonexistent/file.gob")
	if err == nil {
		t.Error("Expected error when loading non-existent file")
	}
}

func TestDataFrame_LoadCorruptedFile(t *testing.T) {
	// Create a temporary file with invalid data
	tempFile, err := os.CreateTemp("", "mframe-corrupted-*.gob")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()

	// Write some garbage data
	_, _ = tempFile.Write([]byte("This is not a valid gob file"))
	_ = tempFile.Close()

	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	err = df.LoadFromFile(tempFile.Name())
	if err == nil {
		t.Error("Expected error when loading corrupted file")
	}
}

func TestDataFrame_SaveLoadWithCleaner(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-cleaner-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create DataFrame with short TTL
	df := &mframe.DataFrame{}
	df.Init(100 * time.Millisecond)
	df.StartCleaner()

	// Insert data
	df.Insert(map[mframe.KeyName]interface{}{
		"test": "data",
	})

	// Save
	filename := filepath.Join(tempDir, "test.gob")
	if err := df.SaveToFile(filename); err != nil {
		t.Fatalf("Failed to save DataFrame: %v", err)
	}

	// Stop cleaner
	df.StopCleaner()

	// Load into new DataFrame
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)
	df2.StartCleaner()

	if err := df2.LoadFromFile(filename); err != nil {
		t.Fatalf("Failed to load DataFrame: %v", err)
	}

	// Verify data exists
	if len(df2.Data) != 1 {
		t.Errorf("Expected 1 row, got %d", len(df2.Data))
	}

	// Stop cleaner
	df2.StopCleaner()
}

func TestDataFrame_SaveLoadWithWriter(t *testing.T) {
	// Create and populate a DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	df.Insert(map[mframe.KeyName]interface{}{
		"name":  "Test",
		"value": float64(42),
	})

	// Save to buffer
	var buf bytes.Buffer
	if err := df.SaveToWriter(&buf); err != nil {
		t.Fatalf("Failed to save to writer: %v", err)
	}

	// Load from buffer
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)

	if err := df2.LoadFromReader(&buf); err != nil {
		t.Fatalf("Failed to load from reader: %v", err)
	}

	// Verify data
	if len(df2.Data) != len(df.Data) {
		t.Errorf("Data length mismatch: got %d, want %d", len(df2.Data), len(df.Data))
	}
}

func TestDataFrame_VersionMismatch(t *testing.T) {
	// Create a buffer to simulate a file with higher version
	var buf bytes.Buffer

	// This would normally be done by gob encoder, but we'll simulate
	// by creating a valid dataframe and modifying its version
	df := &mframe.DataFrame{}
	df.Init(time.Hour)
	df.Version = 999

	if err := df.SaveToWriter(&buf); err != nil {
		t.Fatalf("Failed to create mock data: %v", err)
	}

	// Try to load with normal DataFrame (version 1)
	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)

	err := df2.LoadFromReader(&buf)
	if err == nil {
		t.Error("Expected error for version mismatch")
	}
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("unsupported file version")) {
		t.Errorf("Expected version error, got: %v", err)
	}
}

func TestDataFrame_PreserveAllIndexTypes(t *testing.T) {
	// Create a temporary directory for test files
	tempDir, err := os.MkdirTemp("", "mframe-allindex-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	// Create DataFrame
	df := &mframe.DataFrame{}
	df.Init(time.Hour)

	// Insert data with all types
	now := time.Now().UTC().Truncate(time.Second) // Truncate for consistent comparison
	df.Insert(map[mframe.KeyName]interface{}{
		"string_field":  "test",
		"numeric_field": float64(42.5),
		"bool_field":    true,
		"time_field":    now,
		"uuid_field":    uuid.New(),
		"int_field":     int(10),
		"nested": map[string]interface{}{
			"inner": "value",
		},
	})

	// Save and load
	filename := filepath.Join(tempDir, "allindex.gob")
	if err := df.SaveToFile(filename); err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	df2 := &mframe.DataFrame{}
	df2.Init(time.Hour)
	if err := df2.LoadFromFile(filename); err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	// Verify all index types are preserved
	if _, ok := df2.Keys["string_field"]; !ok {
		t.Error("string_field key not preserved")
	}
	if _, ok := df2.Keys["numeric_field"]; !ok {
		t.Error("numeric_field key not preserved")
	}
	if _, ok := df2.Keys["bool_field"]; !ok {
		t.Error("bool_field key not preserved")
	}
	if _, ok := df2.Keys["time_field"]; !ok {
		t.Error("time_field key not preserved")
	}

	// Verify indexes work
	if len(df2.Strings["string_field"]) != 1 {
		t.Error("String index not preserved correctly")
	}
	if len(df2.Numerics["numeric_field"]) != 1 {
		t.Error("Numeric index not preserved correctly")
	}
	if len(df2.Booleans["bool_field"]) != 1 {
		t.Error("Boolean index not preserved correctly")
	}
	if len(df2.Times["time_field"]) != 1 {
		t.Error("Time index not preserved correctly")
	}

	// Verify nested data
	if len(df2.Strings["nested.inner"]) != 1 {
		t.Error("Nested string index not preserved correctly")
	}
}

func BenchmarkDataFrame_SaveToFile(b *testing.B) {
	// Create temp dir
	tempDir, err := os.MkdirTemp("", "mframe-bench-*")
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

	filename := filepath.Join(tempDir, "bench.gob")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := df.SaveToFile(filename); err != nil {
			b.Fatalf("Failed to save: %v", err)
		}
	}
}

func BenchmarkDataFrame_LoadFromFile(b *testing.B) {
	// Create temp dir
	tempDir, err := os.MkdirTemp("", "mframe-bench-load-*")
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

	filename := filepath.Join(tempDir, "bench.gob")
	if err := df.SaveToFile(filename); err != nil {
		b.Fatalf("Failed to save: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df2 := &mframe.DataFrame{}
		df2.Init(time.Hour)
		if err := df2.LoadFromFile(filename); err != nil {
			b.Fatalf("Failed to load: %v", err)
		}
	}
}
