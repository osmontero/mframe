package mframe

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// index processes key-value pairs recursively to index data into the DataFrame,
// handling various data types and nested structures.
func (d *DataFrame) index(kv map[KeyName]interface{}, wrapKey KeyName, id uuid.UUID, row *Row) {
	for kvKey, kvValue := range kv {
		if wrapKey != "" {
			kvKey = KeyName(fmt.Sprintf("%s.%s", wrapKey, kvKey))
		}

		kvValueType := reflect.TypeOf(kvValue)
		if kvValueType == nil {
			continue
		}

		switch kvValueType.String() {
		case "map[string]interface {}":
			newKv := kvValue.(map[KeyName]interface{})
			d.index(newKv, kvKey, id, row)
		case "[]interface {}":
			for listKey, listValue := range kvValue.([]interface{}) {
				newKv := map[KeyName]interface{}{KeyName(fmt.Sprint(listKey)): listValue}
				d.index(newKv, kvKey, id, row)
			}
		case "string":
			err := d.addMapping(kvKey, String)
			if err != nil {
				log.Printf("error adding mapping for key '%s': %s", kvKey, err.Error())
				continue
			}

			tmpR := *row
			tmpR[kvKey] = kvValue

			if len(d.Strings[kvKey]) == 0 {
				d.Strings[kvKey] = make(map[string]map[uuid.UUID]bool)
			}

			if len(d.Strings[kvKey][kvValue.(string)]) == 0 {
				d.Strings[kvKey][kvValue.(string)] = make(map[uuid.UUID]bool)
			}

			d.Strings[kvKey][kvValue.(string)][id] = false
		case "float64":
			d.num(kvKey, kvValue.(float64), id, row)
		case "int64":
			d.num(kvKey, float64(kvValue.(int64)), id, row)
		case "float":
			d.num(kvKey, float64(kvValue.(float32)), id, row)
		case "int":
			d.num(kvKey, float64(kvValue.(int)), id, row)
		case "bool":
			err := d.addMapping(kvKey, Boolean)
			if err != nil {
				log.Printf("error adding mapping for key '%s': %s", kvKey, err.Error())
				continue
			}

			tmpR := *row
			tmpR[kvKey] = kvValue

			if len(d.Booleans[kvKey]) == 0 {
				d.Booleans[kvKey] = make(map[bool]map[uuid.UUID]bool)
			}

			if len(d.Booleans[kvKey][kvValue.(bool)]) == 0 {
				d.Booleans[kvKey][kvValue.(bool)] = make(map[uuid.UUID]bool)
			}

			d.Booleans[kvKey][kvValue.(bool)][id] = false
		case "uuid.UUID":
			err := d.addMapping(kvKey, String)
			if err != nil {
				log.Printf("error adding mapping for key '%s': %s", kvKey, err.Error())
				continue
			}

			tmpR := *row
			tmpR[kvKey] = kvValue

			if len(d.Strings[kvKey]) == 0 {
				d.Strings[kvKey] = make(map[string]map[uuid.UUID]bool)
			}

			if len(d.Strings[kvKey][kvValue.(string)]) == 0 {
				d.Strings[kvKey][kvValue.(string)] = make(map[uuid.UUID]bool)
			}

			d.Strings[kvKey][kvValue.(string)][id] = false
		case "time.Time":
			err := d.addMapping(kvKey, String)
			if err != nil {
				log.Printf("error adding mapping for key '%s': %s", kvKey, err.Error())
				continue
			}

			tmpR := *row
			tmpR[kvKey] = kvValue

			if len(d.Strings[kvKey]) == 0 {
				d.Strings[kvKey] = make(map[string]map[uuid.UUID]bool)
			}

			if len(d.Strings[kvKey][kvValue.(string)]) == 0 {
				d.Strings[kvKey][kvValue.(string)] = make(map[uuid.UUID]bool)
			}

			d.Strings[kvKey][kvValue.(string)][id] = false
		default:
			log.Printf("unknown field type: %s", kvValueType.String())
		}
	}
}

// num adds a numeric value to the DataFrame using the specified key, value, id, and updates the provided row.
func (d *DataFrame) num(keyName KeyName, value float64, id uuid.UUID, row *Row) {
	err := d.addMapping(keyName, Numeric)
	if err != nil {
		log.Printf("error adding mapping for key '%s': %s", keyName, err.Error())
		return
	}

	tmpR := *row
	tmpR[keyName] = value

	if len(d.Numerics[keyName]) == 0 {
		d.Numerics[keyName] = make(map[float64]map[uuid.UUID]bool)
	}

	if len(d.Numerics[keyName][value]) == 0 {
		d.Numerics[keyName][value] = make(map[uuid.UUID]bool)
	}

	d.Numerics[keyName][value][id] = false
}

// Insert adds a new row to the DataFrame using the provided data,
// generating a unique ID and applying the configured TTL.
func (d *DataFrame) Insert(data map[KeyName]interface{}) {
	d.Locker.Lock()
	defer d.Locker.Unlock()

	id := uuid.New()
	var row = make(Row)
	d.index(data, "", id, &row)
	d.Data[id] = row
	d.ExpireAt[id] = time.Now().UTC().Add(d.TTL)
}

// addMapping maps a keyName to a specified keyType in the DataFrame.
// Returns an error if the keyName already has a different keyType.
func (d *DataFrame) addMapping(keyName KeyName, keyType KeyType) error {
	if key, ok := d.Keys[keyName]; ok && key != keyType {
		return fmt.Errorf("cannot map key '%s' as '%v' because it is already mapped as type '%v'", keyName, keyType, d.Keys[keyName])
	}

	d.Keys[keyName] = keyType

	return nil
}
