package mframe

// ToSlice converts the DataFrame into a slice of Row, preserving the order of rows in the DataFrame.
func (d *DataFrame) ToSlice() []Row {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	var result = make([]Row, 0, len(d.Data))

	for _, row := range d.Data {
		result = append(result, row)
	}

	return result
}

// SliceOf returns a slice of interface values corresponding to the specified field
// (KeyName) from the DataFrame's data rows.
func (d *DataFrame) SliceOf(field KeyName) []interface{} {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return d.sliceOfUnlocked(field)
}

// sliceOfUnlocked returns a slice of interface values without acquiring locks
func (d *DataFrame) sliceOfUnlocked(field KeyName) []interface{} {
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
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return d.sliceOfFloat64Unlocked(field)
}

// sliceOfFloat64Unlocked extracts float64 values without acquiring locks
func (d *DataFrame) sliceOfFloat64Unlocked(field KeyName) []float64 {
	list := d.sliceOfUnlocked(field)

	var fList = make([]float64, 0, len(list))

	for _, value := range list {
		v, ok := value.(float64)
		if !ok {
			continue
		}

		fList = append(fList, v)
	}

	return fList
}
