package mframe

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// KeysBTree is a map of string keys to string values.
type KeysBTree map[string]string

// StringsBTree is a map of string keys to map of string keys to map of UUID keys to boolean values.
type StringsBTree map[string]map[string]map[uuid.UUID]bool

// NumericsBTree is a map of string keys to map of float64 keys to map of UUID keys to boolean values.
type NumericsBTree map[string]map[float64]map[uuid.UUID]bool

// BooleansBTree is a map of string keys to map of boolean keys to map of UUID keys to boolean values.
type BooleansBTree map[string]map[bool]map[uuid.UUID]bool

// ExpireAtBTree is a map of UUID keys to time.Time values.
type ExpireAtBTree map[uuid.UUID]time.Time

// Row is a map of string keys to interface{} values.
type Row map[string]interface{}

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
