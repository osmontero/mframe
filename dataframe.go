package mframe

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type KeyType int

type KeyName string

type FilterOption int

const (
	CaseSensitive FilterOption = 1
)

const (
	String  KeyType = 1
	Numeric KeyType = 2
	Boolean KeyType = 3
)

// KeysBTree is a map of string keys to string values.
type KeysBTree map[KeyName]KeyType

// StringsBTree is a map of string keys to map of string keys to map of UUID keys to boolean values.
type StringsBTree map[KeyName]map[string]map[uuid.UUID]bool

// NumericsBTree is a map of string keys to map of float64 keys to map of UUID keys to boolean values.
type NumericsBTree map[KeyName]map[float64]map[uuid.UUID]bool

// BooleansBTree is a map of string keys to map of boolean keys to map of UUID keys to boolean values.
type BooleansBTree map[KeyName]map[bool]map[uuid.UUID]bool

// ExpireAtBTree is a map of UUID keys to time.Time values.
type ExpireAtBTree map[uuid.UUID]time.Time

// Row is a map of string keys to interface{} values.
type Row map[KeyName]interface{}

// DataFrame is a struct that holds data in a map of UUID keys to Row values, and several B-tree maps for indexing.
type DataFrame struct {
	Data     map[uuid.UUID]Row
	Keys     KeysBTree
	Strings  StringsBTree
	Numerics NumericsBTree
	Booleans BooleansBTree
	ExpireAt ExpireAtBTree
	Locker   sync.RWMutex
	TTL      time.Duration
}

// Init initializes a DataFrame with a given TTL.
func (d *DataFrame) Init(ttl time.Duration) {
	d.Data = make(map[uuid.UUID]Row)
	d.Keys = make(KeysBTree)
	d.Strings = make(StringsBTree)
	d.Numerics = make(NumericsBTree)
	d.Booleans = make(BooleansBTree)
	d.ExpireAt = make(ExpireAtBTree)
	d.TTL = ttl
}
