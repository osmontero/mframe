package mframe

func (d *DataFrame) ToSlice() []map[string]interface{} {
	var result = make([]map[string]interface{}, 0, 1)

	for _, row := range d.Data {
		result = append(result, row)
	}

	return result
}

func (d *DataFrame) SliceOf(field string) []interface{} {
	var list []interface{}
	for _, v := range d.Data {
		value, ok := v[field]
		if !ok {
			continue
		}
		list = append(list, value)
	}

	return list
}

func (d *DataFrame) SliceOfFloat64(field string) []float64 {
	list := d.SliceOf(field)
	

	var fList = []float64{}
	for _, value := range list {
		v, ok := value.(float64)
		if !ok {
			continue
		}
		fList = append(fList, v)
	}
	return fList
}
