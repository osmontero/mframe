package mframe

// Append the rows of a DataFrame to another DataFrame, adding a key column with the specified key value.
func (d *DataFrame) Append(df *DataFrame, key string) {
	df.Locker.RLock()
	defer df.Locker.RUnlock()
	for _, value := range df.Data {
		value["key"] = key
		d.Insert(value)
	}
}
