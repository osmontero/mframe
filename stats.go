package mframe

import (
	"log"
	"time"
)

// Stats periodically logs statistics about the DataFrame.
func (d *DataFrame) Stats(name string) {
	for {
		d.Locker.RLock()

		log.Printf("[%s] data in memory: %d", name, len(d.Data))
		log.Printf("[%s] string indices: %d", name, len(d.Strings))
		log.Printf("[%s] numeric indices: %d", name, len(d.Numerics))
		log.Printf("[%s] boolean indices: %d", name, len(d.Booleans))

		for k1, v1 := range d.Strings {
			log.Printf(`[%s] %d values in '%s' string index`, name, len(v1), k1)
			for k2, v2 := range v1 {
				log.Printf(`[%s] %d IDs for value '%s' in '%s' string index`, name, len(v2), k2, k1)
			}
		}

		for k1, v1 := range d.Numerics {
			log.Printf(`[%s] %d values in '%s' numeric index`, name, len(v1), k1)
			for k2, v2 := range v1 {
				log.Printf(`[%s] %d IDs for value '%f' in '%s' numeric index`, name, len(v2), k2, k1)
			}
		}

		for k1, v1 := range d.Booleans {
			log.Printf(`[%s] %d values in '%s' boolean index`, name, len(v1), k1)
			for k2, v2 := range v1 {
				log.Printf(`[%s] %d IDs for value '%t' in '%s' boolean index`, name, len(v2), k2, k1)
			}
		}

		d.Locker.RUnlock()

		time.Sleep(1 * time.Minute)
	}
}
