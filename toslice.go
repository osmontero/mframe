package mframe

// ToSlice returns a slice of maps representing the DataFrame's data.
// Each map contains the column names as keys and the corresponding row values as values.
func (d *DataFrame) ToSlice() []Row {
	var result = make([]Row, 0, 1)

	for _, row := range d.Data {
		result = append(result, row)
	}

	return result
}

// SliceOf returns a slice of interface{} representing the values of a specific field in the DataFrame.
func (d *DataFrame) SliceOf(field KeyName) []interface{} {
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

// SliceOfFloat64 returns a slice of float64 representing the values of a specific field in the DataFrame.
// If a value cannot be converted to float64, it is skipped.
func (d *DataFrame) SliceOfFloat64(field KeyName) []float64 {
	list := d.SliceOf(field)

	var fList = make([]float64, 0, 1)

	for _, value := range list {
		v, ok := value.(float64)
		if !ok {
			continue
		}

		fList = append(fList, v)
	}

	return fList
}
