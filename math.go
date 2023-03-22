package mframe

import (
	"time"

	"github.com/montanaflynn/stats"
)

func (d *DataFrame) Count(name string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	result.Insert(map[string]interface{}{name: len(d.Data)})

	return result
}

func (d *DataFrame) CountUnique(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)
	var count = make(map[interface{}]int)
	for _, v := range d.Data {
		if _, ok := count[v[field]]; !ok {
			count[v[field]] = 0
		}
		count[v[field]] += 1
	}

	for k, v := range count {
		kv := map[string]interface{}{"value": k, "count": v}
		result.Insert(map[string]interface{}{name: kv})
	}

	return result
}

func (d *DataFrame) Sum(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}
	
	sum, err := stats.Sum(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: sum})

	return result
}

func (d *DataFrame) Average(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	average, err := stats.Mean(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: average})

	return result
}

func (d *DataFrame) Median(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	median, err := stats.Median(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: median})

	return result
}

func (d *DataFrame) Mode(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	mode, err := stats.Mode(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: mode})

	return result
}

func (d *DataFrame) Max(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	max, err := stats.Max(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: max})

	return result
}

func (d *DataFrame) Min(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	min, err := stats.Min(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: min})

	return result
}

func (d *DataFrame) Variance(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	variance, err := stats.Variance(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: variance})

	return result
}
