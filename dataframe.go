package mframe

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// KeyType represents the type of a key, used for distinguishing between different data types in a structure.
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
)

// KeysIndex is a map that associates KeyName keys with their corresponding KeyType values.
type KeysIndex map[KeyName]KeyType

// StringsIndex is a map of KeyName keys to map of string keys to map of UUID keys to boolean values.
type StringsIndex map[KeyName]map[string]map[uuid.UUID]bool

// NumericsIndex is a map of KeyName keys to map of float64 keys to map of UUID keys to boolean values.
type NumericsIndex map[KeyName]map[float64]map[uuid.UUID]bool

// BooleansIndex is a map of KeyName keys to map of boolean keys to map of UUID keys to boolean values.
type BooleansIndex map[KeyName]map[bool]map[uuid.UUID]bool

// ExpireAtIndex is a map that associates UUID keys with their corresponding expiration times as time.Time values.
type ExpireAtIndex map[uuid.UUID]time.Time

// Row represents a single row of data as a map with KeyName keys and interface{} values.
type Row map[KeyName]interface{}

// DataFrame represents a structure for managing indexed data with TTL and thread-safe operations.
type DataFrame struct {
	Data     map[uuid.UUID]Row
	Keys     KeysIndex
	Strings  StringsIndex
	Numerics NumericsIndex
	Booleans BooleansIndex
	ExpireAt ExpireAtIndex
	Locker   sync.RWMutex
	TTL      time.Duration
}

// Init initializes the DataFrame with default indexes, an empty data map, and sets the TTL for data expiration.
func (d *DataFrame) Init(ttl time.Duration) {
	d.Data = make(map[uuid.UUID]Row)
	d.Keys = make(KeysIndex)
	d.Strings = make(StringsIndex)
	d.Numerics = make(NumericsIndex)
	d.Booleans = make(BooleansIndex)
	d.ExpireAt = make(ExpireAtIndex)
	d.TTL = ttl
}
