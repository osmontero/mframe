package mframe

import (
	"github.com/montanaflynn/stats"
)

// Count returns the number of elements in the DataFrame.
func (d *DataFrame) Count() int {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return len(d.Data)
}

// CountUnique counts the occurrences of unique values in the specified field and returns a map of value to its count.
func (d *DataFrame) CountUnique(field KeyName) map[interface{}]int {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	var count = make(map[interface{}]int)
	for _, v := range d.Data {
		if _, ok := count[v[field]]; !ok {
			count[v[field]] = 0
		}
		count[v[field]] += 1

	}

	return count
}

// Sum calculates the sum of all float64 values in the specified field of the DataFrame and returns the result.
func (d *DataFrame) Sum(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Sum(d.sliceOfFloat64Unlocked(field))
}

// Average calculates the mean of the values in the specified field and returns it as a float64 or an error if it fails.
func (d *DataFrame) Average(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Mean(d.sliceOfFloat64Unlocked(field))
}

// Median calculates the median of the values in the specified field and returns it as a float64 along with an error if any.
func (d *DataFrame) Median(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Median(d.sliceOfFloat64Unlocked(field))
}

// Max calculates and returns the maximum value from the specified field in the DataFrame.
func (d *DataFrame) Max(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Max(d.sliceOfFloat64Unlocked(field))
}

// Min computes the minimum value of the specified field in the DataFrame
// and returns it along with any error encountered.
func (d *DataFrame) Min(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Min(d.sliceOfFloat64Unlocked(field))
}

// Variance computes the variance for the specified field in the DataFrame and returns it as a float64 value.
func (d *DataFrame) Variance(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Variance(d.sliceOfFloat64Unlocked(field))
}

// StandardDeviation calculates the standard deviation of the values in the specified field
// and returns it as a float64 along with any error.
func (d *DataFrame) StandardDeviation(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.StandardDeviation(d.sliceOfFloat64Unlocked(field))
}

// Percentile calculates the percentile value for the specified field and percentile (0-100)
// and returns it as a float64 along with any error.
func (d *DataFrame) Percentile(field KeyName, percent float64) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Percentile(d.sliceOfFloat64Unlocked(field), percent)
}

// Mode returns the most frequently occurring value(s) in the specified field.
// Multiple values are returned if there is a tie for the most frequent.
func (d *DataFrame) Mode(field KeyName) ([]float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.Mode(d.sliceOfFloat64Unlocked(field))
}

// Range calculates the difference between the maximum and minimum values
// in the specified field and returns it as a float64.
func (d *DataFrame) Range(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	data := d.sliceOfFloat64Unlocked(field)
	if len(data) == 0 {
		return 0, stats.EmptyInputErr
	}
	minVal, err := stats.Min(data)
	if err != nil {
		return 0, err
	}
	maxVal, err := stats.Max(data)
	if err != nil {
		return 0, err
	}
	return maxVal - minVal, nil
}

// GeometricMean calculates the geometric mean of the values in the specified field
// and returns it as a float64 along with any error.
func (d *DataFrame) GeometricMean(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.GeometricMean(d.sliceOfFloat64Unlocked(field))
}

// HarmonicMean calculates the harmonic mean of the values in the specified field
// and returns it as a float64 along with any error.
func (d *DataFrame) HarmonicMean(field KeyName) (float64, error) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()
	return stats.HarmonicMean(d.sliceOfFloat64Unlocked(field))
}
