package mframe

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

func init() {
	// Register types for gob encoding
	gob.Register(time.Time{})
	gob.Register(uuid.UUID{})
}

// persistentDataFrame is used for serialization. It contains all the data
// needed to reconstruct a DataFrame, excluding non-serializable fields.
type persistentDataFrame struct {
	Version       int
	Data          map[uuid.UUID]Row
	Keys          KeysIndex
	Strings       StringsIndex
	Numerics      NumericsIndex
	Booleans      BooleansIndex
	Times         TimesIndex
	ExpireAt      ExpireAtIndex
	TTL           time.Duration
	MaxRegexCache int
	RegexPatterns []string // Store patterns to recompile after load
}

// SaveToFile saves the DataFrame to a file using gob encoding.
// It performs an atomic write by first writing to a temporary file and then renaming it.
func (d *DataFrame) SaveToFile(filename string) error {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	// Create a persistable version of the DataFrame
	pdf := &persistentDataFrame{
		Version:       d.Version,
		Data:          d.Data,
		Keys:          d.Keys,
		Strings:       d.Strings,
		Numerics:      d.Numerics,
		Booleans:      d.Booleans,
		Times:         d.Times,
		ExpireAt:      d.ExpireAt,
		TTL:           d.TTL,
		MaxRegexCache: d.maxRegexCache,
	}

	// Extract regex patterns
	d.regexMutex.RLock()
	pdf.RegexPatterns = make([]string, 0, len(d.regexCache))
	for pattern := range d.regexCache {
		pdf.RegexPatterns = append(pdf.RegexPatterns, pattern)
	}
	d.regexMutex.RUnlock()

	// Create temporary file in the same directory for atomic write
	dir := filepath.Dir(filename)
	tmpFile, err := os.CreateTemp(dir, ".tmp-mframe-*.gob")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() { _ = os.Remove(tmpName) }() // Clean up temp file if something goes wrong

	// Encode and write to temporary file
	encoder := gob.NewEncoder(tmpFile)
	if err := encoder.Encode(pdf); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to encode dataframe: %w", err)
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

// LoadFromFile loads a DataFrame from a file using gob decoding.
func (d *DataFrame) LoadFromFile(filename string) error {
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

	// Decode the persistent dataframe
	var pdf persistentDataFrame
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&pdf); err != nil {
		return fmt.Errorf("failed to decode dataframe: %w", err)
	}

	// Validate version
	if pdf.Version > d.Version {
		return fmt.Errorf("unsupported file version %d (current version is %d)", pdf.Version, d.Version)
	}

	// Clear existing data
	d.Data = pdf.Data
	d.Keys = pdf.Keys
	d.Strings = pdf.Strings
	d.Numerics = pdf.Numerics
	d.Booleans = pdf.Booleans
	d.Times = pdf.Times
	d.ExpireAt = pdf.ExpireAt
	d.TTL = pdf.TTL
	d.maxRegexCache = pdf.MaxRegexCache

	// Re-initialize non-serializable fields
	d.regexCache = make(map[string]*regexp.Regexp)
	d.regexCacheSize = 0
	d.regexMutex = sync.RWMutex{}
	d.stopCleaner = make(chan bool)

	// Recompile regex patterns
	for _, pattern := range pdf.RegexPatterns {
		if re, err := regexp.Compile(pattern); err == nil {
			d.regexCache[pattern] = re
			d.regexCacheSize++
			if d.regexCacheSize >= d.maxRegexCache {
				break
			}
		}
	}

	// Restart cleaner if it was running
	if wasCleanerRunning {
		go d.CleanExpired()
	}

	return nil
}

// SaveToFileCompressed saves the DataFrame to a gzip-compressed file.
func (d *DataFrame) SaveToFileCompressed(filename string) error {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	// Create a persistable version of the DataFrame
	pdf := &persistentDataFrame{
		Version:       d.Version,
		Data:          d.Data,
		Keys:          d.Keys,
		Strings:       d.Strings,
		Numerics:      d.Numerics,
		Booleans:      d.Booleans,
		Times:         d.Times,
		ExpireAt:      d.ExpireAt,
		TTL:           d.TTL,
		MaxRegexCache: d.maxRegexCache,
	}

	// Extract regex patterns
	d.regexMutex.RLock()
	pdf.RegexPatterns = make([]string, 0, len(d.regexCache))
	for pattern := range d.regexCache {
		pdf.RegexPatterns = append(pdf.RegexPatterns, pattern)
	}
	d.regexMutex.RUnlock()

	// Create temporary file in the same directory for atomic write
	dir := filepath.Dir(filename)
	tmpFile, err := os.CreateTemp(dir, ".tmp-mframe-*.gob.gz")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() { _ = os.Remove(tmpName) }() // Clean up temp file if something goes wrong

	// Create gzip writer
	gzWriter := gzip.NewWriter(tmpFile)

	// Encode and write to gzip writer
	encoder := gob.NewEncoder(gzWriter)
	if err := encoder.Encode(pdf); err != nil {
		_ = gzWriter.Close()
		_ = tmpFile.Close()
		return fmt.Errorf("failed to encode dataframe: %w", err)
	}

	// Close gzip writer
	if err := gzWriter.Close(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("failed to close gzip writer: %w", err)
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

// LoadFromFileCompressed loads a DataFrame from a gzip-compressed file.
func (d *DataFrame) LoadFromFileCompressed(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Create gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer func() { _ = gzReader.Close() }()

	// Stop the cleaner if it's running
	wasCleanerRunning := false
	select {
	case d.stopCleaner <- true:
		wasCleanerRunning = true
	default:
	}

	d.Locker.Lock()
	defer d.Locker.Unlock()

	// Decode the persistent dataframe
	var pdf persistentDataFrame
	decoder := gob.NewDecoder(gzReader)
	if err := decoder.Decode(&pdf); err != nil {
		return fmt.Errorf("failed to decode dataframe: %w", err)
	}

	// Validate version
	if pdf.Version > d.Version {
		return fmt.Errorf("unsupported file version %d (current version is %d)", pdf.Version, d.Version)
	}

	// Clear existing data
	d.Data = pdf.Data
	d.Keys = pdf.Keys
	d.Strings = pdf.Strings
	d.Numerics = pdf.Numerics
	d.Booleans = pdf.Booleans
	d.Times = pdf.Times
	d.ExpireAt = pdf.ExpireAt
	d.TTL = pdf.TTL
	d.maxRegexCache = pdf.MaxRegexCache

	// Re-initialize non-serializable fields
	d.regexCache = make(map[string]*regexp.Regexp)
	d.regexCacheSize = 0
	d.regexMutex = sync.RWMutex{}
	d.stopCleaner = make(chan bool)

	// Recompile regex patterns
	for _, pattern := range pdf.RegexPatterns {
		if re, err := regexp.Compile(pattern); err == nil {
			d.regexCache[pattern] = re
			d.regexCacheSize++
			if d.regexCacheSize >= d.maxRegexCache {
				break
			}
		}
	}

	// Restart cleaner if it was running
	if wasCleanerRunning {
		go d.CleanExpired()
	}

	return nil
}

// SaveToWriter saves the DataFrame to an io.Writer using gob encoding.
func (d *DataFrame) SaveToWriter(w io.Writer) error {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	// Create a persistable version of the DataFrame
	pdf := &persistentDataFrame{
		Version:       d.Version,
		Data:          d.Data,
		Keys:          d.Keys,
		Strings:       d.Strings,
		Numerics:      d.Numerics,
		Booleans:      d.Booleans,
		Times:         d.Times,
		ExpireAt:      d.ExpireAt,
		TTL:           d.TTL,
		MaxRegexCache: d.maxRegexCache,
	}

	// Extract regex patterns
	d.regexMutex.RLock()
	pdf.RegexPatterns = make([]string, 0, len(d.regexCache))
	for pattern := range d.regexCache {
		pdf.RegexPatterns = append(pdf.RegexPatterns, pattern)
	}
	d.regexMutex.RUnlock()

	// Encode
	encoder := gob.NewEncoder(w)
	return encoder.Encode(pdf)
}

// LoadFromReader loads a DataFrame from an io.Reader using gob decoding.
func (d *DataFrame) LoadFromReader(r io.Reader) error {
	// Stop the cleaner if it's running
	wasCleanerRunning := false
	select {
	case d.stopCleaner <- true:
		wasCleanerRunning = true
	default:
	}

	d.Locker.Lock()
	defer d.Locker.Unlock()

	// Decode the persistent dataframe
	var pdf persistentDataFrame
	decoder := gob.NewDecoder(r)
	if err := decoder.Decode(&pdf); err != nil {
		return fmt.Errorf("failed to decode dataframe: %w", err)
	}

	// Validate version
	if pdf.Version > d.Version {
		return fmt.Errorf("unsupported file version %d (current version is %d)", pdf.Version, d.Version)
	}

	// Clear existing data
	d.Data = pdf.Data
	d.Keys = pdf.Keys
	d.Strings = pdf.Strings
	d.Numerics = pdf.Numerics
	d.Booleans = pdf.Booleans
	d.Times = pdf.Times
	d.ExpireAt = pdf.ExpireAt
	d.TTL = pdf.TTL
	d.maxRegexCache = pdf.MaxRegexCache

	// Re-initialize non-serializable fields
	d.regexCache = make(map[string]*regexp.Regexp)
	d.regexCacheSize = 0
	d.regexMutex = sync.RWMutex{}
	d.stopCleaner = make(chan bool)

	// Recompile regex patterns
	for _, pattern := range pdf.RegexPatterns {
		if re, err := regexp.Compile(pattern); err == nil {
			d.regexCache[pattern] = re
			d.regexCacheSize++
			if d.regexCacheSize >= d.maxRegexCache {
				break
			}
		}
	}

	// Restart cleaner if it was running
	if wasCleanerRunning {
		go d.CleanExpired()
	}

	return nil
}
