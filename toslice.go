package mframe

// ToSlice converts the DataFrame into a slice of Row, preserving the order of rows in the DataFrame.
func (d *DataFrame) ToSlice() []Row {
	var result = make([]Row, 0, 1)

	for _, row := range d.Data {
		result = append(result, row)
	}

	return result
}

// SliceOf returns a slice of interface values corresponding to the specified field
// (KeyName) from the DataFrame's data rows.
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

// SliceOfFloat64 extracts and returns a slice of float64 values from the specified field in the DataFrame.
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
