package mframe

func (d *DataFrame) Append(df *DataFrame, key string) {
	df.Locker.RLock()
	for _, value := range df.Data {
		value["key"] = key
		d.Insert(value)
	}
	df.Locker.RUnlock()
}
