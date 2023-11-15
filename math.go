package mframe

import (
	"time"

	"github.com/montanaflynn/stats"
)

// Count returns a new DataFrame with the count of rows in the original DataFrame.
// The name parameter is used to set the name of the new column with the count value.
func (d *DataFrame) Count(name string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	result.Insert(map[string]interface{}{name: len(d.Data)})

	return result
}

// CountUnique returns a new DataFrame with the count of unique values in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the count value.
// The field parameter is used to specify the name of the column to count unique values from.
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
		result.Insert(kv)
	}

	return result
}

// Sum returns a new DataFrame with the sum of values in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the sum value.
// The field parameter is used to specify the name of the column to sum values from.
func (d *DataFrame) Sum(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList := d.SliceOfFloat64(field)

	sum, err := stats.Sum(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: sum})

	return result
}

// Average returns a new DataFrame with the average of values in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the average value.
// The field parameter is used to specify the name of the column to calculate the average from.
func (d *DataFrame) Average(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList := d.SliceOfFloat64(field)

	average, err := stats.Mean(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: average})

	return result
}

// Median returns a new DataFrame with the median of values in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the median value.
// The field parameter is used to specify the name of the column to calculate the median from.
func (d *DataFrame) Median(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList := d.SliceOfFloat64(field)

	median, err := stats.Median(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: median})

	return result
}

// Max returns a new DataFrame with the maximum value in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the maximum value.
// The field parameter is used to specify the name of the column to find the maximum value from.
func (d *DataFrame) Max(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList := d.SliceOfFloat64(field)

	max, err := stats.Max(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: max})

	return result
}

// Min returns a new DataFrame with the minimum value in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the minimum value.
// The field parameter is used to specify the name of the column to find the minimum value from.
func (d *DataFrame) Min(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList := d.SliceOfFloat64(field)

	min, err := stats.Min(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: min})

	return result
}

// Variance returns a new DataFrame with the variance of values in a column of the original DataFrame.
// The name parameter is used to set the name of the new column with the variance value.
// The field parameter is used to specify the name of the column to calculate the variance from.
func (d *DataFrame) Variance(name, field string) *DataFrame {
	var result = new(DataFrame)
	result.Init(10 * time.Minute)

	fList := d.SliceOfFloat64(field)

	variance, err := stats.Variance(fList)
	if err != nil {
		return result
	}

	result.Insert(map[string]interface{}{name: variance})

	return result
}
