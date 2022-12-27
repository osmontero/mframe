package mframe

import (
	"time"

	"github.com/quantfall/rerror"
)

func (d *DataFrame) Stats(name string) {
	for {
		d.Locker.RLock()

		rerror.LogF(200, "[%s] data in memory: %d", name, len(d.Data))
		rerror.LogF(200, "[%s] string indices: %d", name, len(d.Strings))
		rerror.LogF(200, "[%s] numeric indices: %d", name, len(d.Numerics))
		rerror.LogF(200, "[%s] boolean indices: %d", name, len(d.Booleans))

		for k1, v1 := range d.Strings {
			rerror.LogF(200, `[%s] %d values in '%s' string index`, name, len(v1), k1)
			for k2, v2 := range v1 {
				rerror.LogF(200, `[%s] %d IDs for value '%s' in '%s' string index`, name, len(v2), k2, k1)
			}
		}

		for k1, v1 := range d.Numerics {
			rerror.LogF(200, `[%s] %d values in '%s' numeric index`, name, len(v1), k1)
			for k2, v2 := range v1 {
				rerror.LogF(200, `[%s] %d IDs for value '%f' in '%s' numeric index`, name, len(v2), k2, k1)
			}
		}

		for k1, v1 := range d.Booleans {
			rerror.LogF(200, `[%s] %d values in '%s' boolean index`, name, len(v1), k1)
			for k2, v2 := range v1 {
				rerror.LogF(200, `[%s] %d IDs for value '%t' in '%s' boolean index`, name, len(v2), k2, k1)
			}
		}

		d.Locker.RUnlock()
		time.Sleep(1 * time.Minute)
	}
}
