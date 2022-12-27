package mframe

import (
	"time"

	"github.com/montanaflynn/stats"
)

func (d *DataFrame) Count() *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	result.Insert(map[string]interface{}{"value": len(d.Data)})

	return result
}

func (d *DataFrame) CountUnique(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)
	var count = make(map[interface{}]int)
	for _, v := range d.Data {
		if _, ok := count[v[field]]; !ok {
			count[v[field]] = 0
		}
		count[v[field]] += 1
	}

	for k, v := range count {
		result.Insert(map[string]interface{}{"data": k, "value": v})
	}

	return result
}

func (d *DataFrame) Sum(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}
	
	sum, err := stats.Sum(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": sum})

	return result
}

func (d *DataFrame) Average(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	average, err := stats.Mean(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": average})

	return result
}

func (d *DataFrame) Median(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	median, err := stats.Median(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": median})

	return result
}

func (d *DataFrame) Mode(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	mode, err := stats.Mode(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": mode})

	return result
}

func (d *DataFrame) Max(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	max, err := stats.Max(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": max})

	return result
}

func (d *DataFrame) Min(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	min, err := stats.Min(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": min})

	return result
}

func (d *DataFrame) Variance(field string) *DataFrame {
	var result *DataFrame
	result.Init(10 * time.Minute)

	fList, e := d.SliceOfFloat64(field)
	if e != nil {
		return result
	}

	variance, err := stats.Variance(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{"value": variance})

	return result
}
