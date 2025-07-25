package mframe

import (
	"time"

	"github.com/google/uuid"
)

// CleanExpired removes elements from the DataFrame whose expiration time has passed. It runs continuously in a loop.
func (d *DataFrame) CleanExpired() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCleaner:
			return
		case <-ticker.C:
			now := time.Now().UTC()

			var toRemove []uuid.UUID

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
		}
	}
}

// RemoveElement removes the element with the specified UUID from all internal data structures in the DataFrame.
func (d *DataFrame) RemoveElement(id uuid.UUID) {
	d.Locker.Lock()
	defer d.Locker.Unlock()

	delete(d.ExpireAt, id)
	delete(d.Data, id)

	// Track which keys are still in use
	keysInUse := make(map[KeyName]bool)

	for k1, v1 := range d.Strings {
		for k2, v2 := range v1 {
			delete(v2, id)
			if len(v2) == 0 {
				delete(v1, k2)
			}
		}
		if len(v1) == 0 {
			delete(d.Strings, k1)
		} else {
			keysInUse[k1] = true
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
		} else {
			keysInUse[k1] = true
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
		} else {
			keysInUse[k1] = true
		}
	}

	// Clean up Keys index for keys that are no longer in use
	for key := range d.Keys {
		if !keysInUse[key] {
			delete(d.Keys, key)
		}
	}
}
