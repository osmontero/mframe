package mframe

import (
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
)

// KeyType represents the type of key, used for distinguishing between different data types in a structure.
type KeyType int

// KeyName represents a type for defining keys in various data structures.
type KeyName string

// FilterOption defines an integer-based type for specifying filter behavior or options in the application.
type FilterOption int

const (
	CaseSensitive FilterOption = 1
)

const (
	String  KeyType = 1
	Numeric KeyType = 2
	Boolean KeyType = 3
	Time    KeyType = 4
)

// KeysIndex is a map that associates KeyName keys with their corresponding KeyType values.
type KeysIndex map[KeyName]KeyType

// StringsIndex is a map of KeyName keys to map of string keys to map of UUID keys to boolean values.
type StringsIndex map[KeyName]map[string]map[uuid.UUID]bool

// NumericsIndex is a map of KeyName keys to map of float64 keys to map of UUID keys to boolean values.
type NumericsIndex map[KeyName]map[float64]map[uuid.UUID]bool

// BooleansIndex is a map of KeyName keys to map of boolean keys to map of UUID keys to boolean values.
type BooleansIndex map[KeyName]map[bool]map[uuid.UUID]bool

// TimesIndex is a map of KeyName keys to map of time.Time keys to map of UUID keys to boolean values.
type TimesIndex map[KeyName]map[time.Time]map[uuid.UUID]bool

// ExpireAtIndex is a map that associates UUID keys with their corresponding expiration times as time.Time values.
type ExpireAtIndex map[uuid.UUID]time.Time

// Row represents a single row of data as a map with KeyName keys and interface{} values.
type Row map[KeyName]interface{}

// DataFrame represents a structure for managing indexed data with TTL and thread-safe operations.
type DataFrame struct {
	Data           map[uuid.UUID]Row
	Keys           KeysIndex
	Strings        StringsIndex
	Numerics       NumericsIndex
	Booleans       BooleansIndex
	Times          TimesIndex
	ExpireAt       ExpireAtIndex
	Locker         sync.RWMutex
	TTL            time.Duration
	regexCache     map[string]*regexp.Regexp
	regexMutex     sync.RWMutex
	regexCacheSize int
	maxRegexCache  int
	stopCleaner    chan bool
	Version        int // For persistence format versioning
}

// Init initializes the DataFrame with default indexes, an empty data map, and sets the TTL for data expiration.
func (d *DataFrame) Init(ttl time.Duration) {
	d.Data = make(map[uuid.UUID]Row)
	d.Keys = make(KeysIndex)
	d.Strings = make(StringsIndex)
	d.Numerics = make(NumericsIndex)
	d.Booleans = make(BooleansIndex)
	d.Times = make(TimesIndex)
	d.ExpireAt = make(ExpireAtIndex)
	d.TTL = ttl
	d.regexCache = make(map[string]*regexp.Regexp)
	d.maxRegexCache = 1000 // Default cache size
	d.stopCleaner = make(chan bool)
	d.Version = 1 // Current persistence format version
}

// InitWithOptions initializes the DataFrame with custom options.
func (d *DataFrame) InitWithOptions(ttl time.Duration, maxRegexCache int) {
	d.Init(ttl)
	if maxRegexCache > 0 {
		d.maxRegexCache = maxRegexCache
	}
}

// StartCleaner starts the background goroutine for cleaning expired entries
func (d *DataFrame) StartCleaner() {
	go d.CleanExpired()
}

// StopCleaner stops the background cleaner goroutine
func (d *DataFrame) StopCleaner() {
	if d.stopCleaner != nil {
		d.stopCleaner <- true
	}
}

// getCompiledRegex returns a compiled regular expression from cache or compiles and caches it
func (d *DataFrame) getCompiledRegex(pattern string) (*regexp.Regexp, error) {
	d.regexMutex.RLock()
	re, exists := d.regexCache[pattern]
	d.regexMutex.RUnlock()

	if exists {
		return re, nil
	}

	// Compile the regex
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// Cache the compiled regex with size limit
	d.regexMutex.Lock()
	defer d.regexMutex.Unlock()

	// Check cache size and evict oldest if needed
	if d.regexCacheSize >= d.maxRegexCache {
		// Simple eviction: remove one random entry
		for k := range d.regexCache {
			delete(d.regexCache, k)
			d.regexCacheSize--
			break
		}
	}

	d.regexCache[pattern] = compiled
	d.regexCacheSize++

	return compiled, nil
}

// ClearRegexCache clears the regex cache to free memory.
func (d *DataFrame) ClearRegexCache() {
	d.regexMutex.Lock()
	d.regexCache = make(map[string]*regexp.Regexp)
	d.regexCacheSize = 0
	d.regexMutex.Unlock()
}
