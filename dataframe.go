package mframe

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type KeysBTree map[string]string
type StringsBTree map[string]map[string]map[uuid.UUID]bool
type NumericsBTree map[string]map[float64]map[uuid.UUID]bool
type BooleansBTree map[string]map[bool]map[uuid.UUID]bool
type ExpireAtBTree map[uuid.UUID]time.Time

type Row map[string]interface{}

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

func (d *DataFrame) Init(ttl time.Duration) {
	d.Data = make(map[uuid.UUID]Row)
	d.Keys = make(KeysBTree)
	d.Strings = make(StringsBTree)
	d.Numerics = make(NumericsBTree)
	d.Booleans = make(BooleansBTree)
	d.ExpireAt = make(ExpireAtBTree)
	d.TTL = ttl
}
