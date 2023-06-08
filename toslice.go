package mframe

import (
	"fmt"
)

func (d *DataFrame) ToSlice() []map[string]interface{} {
	var result = make([]map[string]interface{}, 0, 1)

	for _, row := range d.Data {
		result = append(result, row)
	}

	return result
}

func (d *DataFrame) SliceOf(field string) ([]interface{}, error) {
	var list []interface{}
	for _, v := range d.Data {
		value, ok := v[field]
		if !ok {
			return []interface{}{}, fmt.Errorf("field '%s' not found in log '%v'", field, v)
		}
		list = append(list, value)
	}

	return list, nil
}

func (d *DataFrame) SliceOfFloat64(field string) ([]float64, error) {
	list, e := d.SliceOf(field)
	if e != nil {
		return []float64{}, e
	}

	var fList = []float64{}
	for _, value := range list {
		v, ok := value.(float64)
		if !ok {
			return []float64{}, fmt.Errorf("'%v' is not type of float64", value)
		}
		fList = append(fList, v)
	}
	return fList, nil
}
