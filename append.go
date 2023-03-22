package mframe

func (d *DataFrame) Append(df *DataFrame, key string) {
	df.Locker.RLock()
	defer df.Locker.RUnlock()
	for _, value := range df.Data {
		value["key"] = key
		d.Insert(value)
	}
}
