package mframe

import (
	"github.com/montanaflynn/stats"
)

func (d *DataFrame) Count() int {
	return len(d.Data)
}

func (d *DataFrame) CountUnique(field KeyName) map[interface{}]int {
	var count = make(map[interface{}]int)
	for _, v := range d.Data {
		if _, ok := count[v[field]]; !ok {
			count[v[field]] = 0
		}
		count[v[field]] += 1

	}

	return count
}

func (d *DataFrame) Sum(field KeyName) (float64, error) {
	return stats.Sum(d.SliceOfFloat64(field))
}

func (d *DataFrame) Average(field KeyName) (float64, error) {
	return stats.Mean(d.SliceOfFloat64(field))
}

func (d *DataFrame) Median(field KeyName) (float64, error) {
	return stats.Median(d.SliceOfFloat64(field))
}

func (d *DataFrame) Max(field KeyName) (float64, error) {
	return stats.Max(d.SliceOfFloat64(field))
}

func (d *DataFrame) Min(field KeyName) (float64, error) {
	return stats.Min(d.SliceOfFloat64(field))
}

func (d *DataFrame) Variance(field KeyName) (float64, error) {
	return stats.Variance(d.SliceOfFloat64(field))
}
