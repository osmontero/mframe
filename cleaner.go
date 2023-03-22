package mframe

import (
	"time"

	"github.com/google/uuid"
)

func (d *DataFrame) CleanExpired() {
	for {
		now := time.Now().UTC()
		var toRemove = []uuid.UUID{}
		d.Locker.RLock()
		for k, v := range d.ExpireAt {
			if v.Before(now) {
				toRemove = append(toRemove, k)
			}
		}
		d.Locker.RUnlock()

		for _, id := range toRemove {
			d.RemoveElement(id)
		}
		time.Sleep(1 * time.Second)
	}
}

func (d *DataFrame) RemoveElement(id uuid.UUID) {
	d.Locker.Lock()
	defer d.Locker.Unlock()
	
	delete(d.ExpireAt, id)
	delete(d.Data, id)

	for k1, v1 := range d.Strings {
		for k2, v2 := range v1 {
			delete(v2, id)
			if len(v2) == 0 {
				delete(v1, k2)
			}
		}
		if len(v1) == 0 {
			delete(d.Strings, k1)
		}
	}

	for k1, v1 := range d.Numerics {
		for k2, v2 := range v1 {
			delete(v2, id)
			if len(v2) == 0 {
				delete(v1, k2)
			}
		}
		if len(v1) == 0 {
			delete(d.Numerics, k1)
		}
	}

	for k1, v1 := range d.Booleans {
		for k2, v2 := range v1 {
			delete(v2, id)
			if len(v2) == 0 {
				delete(v1, k2)
			}
		}
		if len(v1) == 0 {
			delete(d.Booleans, k1)
		}
	}
}
