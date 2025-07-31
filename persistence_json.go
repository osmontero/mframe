package mframe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// jsonDataFrame represents the JSON-serializable structure of a DataFrame
type jsonDataFrame struct {
	Version  int                               `json:"version"`
	Data     map[string]map[string]interface{} `json:"data"`
	Keys     map[string]int                    `json:"keys"`
	ExpireAt map[string]string                 `json:"expire_at"`
	TTL      string                            `json:"ttl"`
}

// ExportToJSON exports the DataFrame to a JSON file for human-readable inspection
func (d *DataFrame) ExportToJSON(filename string) error {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	// Create JSON structure
	jdf := jsonDataFrame{
		Version:  d.Version,
		Data:     make(map[string]map[string]interface{}),
		Keys:     make(map[string]int),
		ExpireAt: make(map[string]string),
		TTL:      d.TTL.String(),
	}

	// Convert UUIDs to strings for JSON
	for id, row := range d.Data {
		rowData := make(map[string]interface{})
		for key, value := range row {
			// Convert KeyName to string
			keyStr := string(key)

			// Handle special types that need conversion
			switch v := value.(type) {
			case time.Time:
				rowData[keyStr] = v.Format(time.RFC3339Nano)
			case uuid.UUID:
				rowData[keyStr] = v.String()
			default:
				rowData[keyStr] = v
			}
		}
		jdf.Data[id.String()] = rowData
	}

	// Convert Keys
	for key, keyType := range d.Keys {
		jdf.Keys[string(key)] = int(keyType)
	}

	// Convert ExpireAt
	for id, expireTime := range d.ExpireAt {
		jdf.ExpireAt[id.String()] = expireTime.Format(time.RFC3339Nano)
	}

	// Create a temporary file in the same directory for atomic writing
	dir := filepath.Dir(filename)
	tmpFile, err := os.CreateTemp(dir, ".tmp-mframe-*.json")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() { _ = os.Remove(tmpName) }()

	// Encode with indentation for readability
	encoder := json.NewEncoder(tmpFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(jdf); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to encode to JSON: %w", err)
	}

	// Close the temporary file
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpName, filename); err != nil {
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

// ImportFromJSON imports a DataFrame from a JSON file
func (d *DataFrame) ImportFromJSON(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Stop the cleaner if it's running
	wasCleanerRunning := false
	select {
	case d.stopCleaner <- true:
		wasCleanerRunning = true
	default:
	}

	d.Locker.Lock()
	defer d.Locker.Unlock()

	// Decode JSON
	var jdf jsonDataFrame
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&jdf); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	// Validate version
	if jdf.Version > d.Version {
		return fmt.Errorf("unsupported file version %d (current version is %d)", jdf.Version, d.Version)
	}

	// Parse TTL
	ttl, err := time.ParseDuration(jdf.TTL)
	if err != nil {
		return fmt.Errorf("failed to parse TTL: %w", err)
	}

	// Clear and reinitialize
	d.Data = make(map[uuid.UUID]Row)
	d.Keys = make(KeysIndex)
	d.Strings = make(StringsIndex)
	d.Numerics = make(NumericsIndex)
	d.Booleans = make(BooleansIndex)
	d.Times = make(TimesIndex)
	d.ExpireAt = make(ExpireAtIndex)
	d.TTL = ttl

	// Re-initialize non-serializable fields
	d.regexCache = make(map[string]*regexp.Regexp)
	d.regexCacheSize = 0
	d.stopCleaner = make(chan bool)

	// Convert Keys back
	for keyStr, keyType := range jdf.Keys {
		d.Keys[KeyName(keyStr)] = KeyType(keyType)
	}

	// Convert Data and rebuild indexes
	for idStr, rowData := range jdf.Data {
		id, err := uuid.Parse(idStr)
		if err != nil {
			return fmt.Errorf("failed to parse UUID %s: %w", idStr, err)
		}

		// Convert row data
		row := make(Row)
		convertedData := make(map[KeyName]interface{})

		for keyStr, value := range rowData {
			key := KeyName(keyStr)

			// Check if this is a known key and handle type conversion
			if keyType, exists := d.Keys[key]; exists {
				switch keyType {
				case Time:
					// Try to parse as time
					if strVal, ok := value.(string); ok {
						if t, err := time.Parse(time.RFC3339Nano, strVal); err == nil {
							convertedData[key] = t
							continue
						}
					}
				case String:
					// For UUID fields stored as strings, keep them as strings
					if strVal, ok := value.(string); ok {
						convertedData[key] = strVal
						continue
					}
				}
			}

			// Default: use value as-is
			convertedData[key] = value
		}

		// Index the data
		d.index(convertedData, "", id, &row)
		d.Data[id] = row
	}

	// Convert ExpireAt
	for idStr, expireStr := range jdf.ExpireAt {
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue // Skip invalid UUIDs
		}

		expireTime, err := time.Parse(time.RFC3339Nano, expireStr)
		if err != nil {
			continue // Skip invalid times
		}

		d.ExpireAt[id] = expireTime
	}

	// Restart cleaner if it was running
	if wasCleanerRunning {
		go d.CleanExpired()
	}

	return nil
}
