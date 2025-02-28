package mframe

import (
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/google/uuid"
)

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
			d.addMapping(kvKey, String)

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
			d.addMapping(kvKey, Boolean)

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
			d.addMapping(kvKey, String)

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
			d.addMapping(kvKey, String)

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

func (d *DataFrame) num(keyName KeyName, value float64, id uuid.UUID, row *Row) {
	d.addMapping(keyName, Numeric)

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

// Insert adds a new row to the DataFrame with the given data.
// The data is a map of string keys to interface{} values.
// The function indexes the data and adds it to the DataFrame.
// The function also generates a new UUID for the row and sets its expiration time.
// The function is thread-safe and uses a mutex to protect the DataFrame from concurrent writings.
func (d *DataFrame) Insert(data map[KeyName]interface{}) {
	d.Locker.Lock()
	defer d.Locker.Unlock()

	id := uuid.New()
	var row = make(Row)
	d.index(data, "", id, &row)
	d.Data[id] = row
	d.ExpireAt[id] = time.Now().UTC().Add(d.TTL)
}

func (d *DataFrame) addMapping(keyName KeyName, keyType KeyType) {
	if key, ok := d.Keys[keyName]; ok && key != keyType {
		log.Printf("cannot map key '%s' as '%v' because it is already mapped as type '%v'", keyName, keyType, d.Keys[keyName])
		return
	}

	d.Keys[keyName] = keyType
}
