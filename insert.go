package mframe

import (
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/quantfall/rerror"
	"google.golang.org/grpc/codes"
)

func (d *DataFrame) index(f map[string]interface{}, k string, id uuid.UUID, r *Row) {
	for key, value := range f {
		if k != "" {
			key = fmt.Sprintf("%s.%s", k, key)
		}

		t := reflect.TypeOf(value)
		switch t.String() {
		case "map[string]interface {}":
			n := value.(map[string]interface{})
			d.index(n, key, id, r)
		case "[]interface {}":
			for k, v := range value.([]interface{}) {
				n := map[string]interface{}{fmt.Sprint(k): v}
				d.index(n, key, id, r)
			}
		case "string":
			d.addMapping(key, "string")

			tmpR := *r
			tmpR[key] = value

			if len(d.Strings[key]) == 0 {
				d.Strings[key] = make(map[string]map[uuid.UUID]bool)
			}

			if len(d.Strings[key][value.(string)]) == 0 {
				d.Strings[key][value.(string)] = make(map[uuid.UUID]bool)
			}

			d.Strings[key][value.(string)][id] = false
		case "float64":
			d.addMapping(key, "numeric")

			tmpR := *r
			tmpR[key] = value

			if len(d.Numerics[key]) == 0 {
				d.Numerics[key] = make(map[float64]map[uuid.UUID]bool)
			}

			if len(d.Numerics[key][value.(float64)]) == 0 {
				d.Numerics[key][value.(float64)] = make(map[uuid.UUID]bool)
			}

			d.Numerics[key][value.(float64)][id] = false
		case "bool":
			d.addMapping(key, "boolean")

			tmpR := *r
			tmpR[key] = value

			if len(d.Booleans[key]) == 0 {
				d.Booleans[key] = make(map[bool]map[uuid.UUID]bool)
			}

			if len(d.Booleans[key][value.(bool)]) == 0 {
				d.Booleans[key][value.(bool)] = make(map[uuid.UUID]bool)
			}

			d.Booleans[key][value.(bool)][id] = false
		case "uuid.UUID":
			d.addMapping(key, "string")

			tmpR := *r
			tmpR[key] = value

			if len(d.Strings[key]) == 0 {
				d.Strings[key] = make(map[string]map[uuid.UUID]bool)
			}

			if len(d.Strings[key][value.(string)]) == 0 {
				d.Strings[key][value.(string)] = make(map[uuid.UUID]bool)
			}

			d.Strings[key][value.(string)][id] = false
		case "time.Time":
			d.addMapping(key, "string")

			tmpR := *r
			tmpR[key] = value

			if len(d.Strings[key]) == 0 {
				d.Strings[key] = make(map[string]map[uuid.UUID]bool)
			}

			if len(d.Strings[key][value.(string)]) == 0 {
				d.Strings[key][value.(string)] = make(map[uuid.UUID]bool)
			}

			d.Strings[key][value.(string)][id] = false
		default:
			rerror.ErrorF(http.StatusBadRequest, codes.InvalidArgument, "unknown field type: %s", t.String())
		}
	}
}

func (d *DataFrame) Insert(data map[string]interface{}) {
	d.Locker.Lock()
	defer d.Locker.Unlock()
	
	id := uuid.New()
	var row = make(Row)
	d.index(data, "", id, &row)
	d.Data[id] = row
	d.ExpireAt[id] = time.Now().UTC().Add(d.TTL)
}

func (d *DataFrame) addMapping(key, kind string) {
	if k, ok := d.Keys[key]; ok && k != kind {
		rerror.ErrorF(http.StatusBadRequest, codes.InvalidArgument, "cannot map key '%s' as '%s' because it is already mapped as type '%s'", key, kind, d.Keys[key])
		return
	}

	d.Keys[key] = kind
}
