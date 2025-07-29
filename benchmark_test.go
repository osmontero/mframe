package mframe_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/threatwinds/mframe"
)

func BenchmarkInsert(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	data := make(map[mframe.KeyName]interface{})
	data["name"] = "test"
	data["value"] = 123.45
	data["active"] = true

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		df.Insert(data)
	}
}

func BenchmarkInsertBatch(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	batchSizes := []int{10, 100, 1000}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", size), func(b *testing.B) {
			batch := make([]map[mframe.KeyName]interface{}, size)
			for i := 0; i < size; i++ {
				batch[i] = map[mframe.KeyName]interface{}{
					"name":   fmt.Sprintf("test%d", i),
					"value":  rand.Float64() * 1000,
					"active": i%2 == 0,
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				df.InsertBatch(batch)
			}
		})
	}
}

func BenchmarkFilterEquals(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	// Insert test data
	for i := 0; i < 10000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":     i,
			"name":   fmt.Sprintf("name_%d", i%100),
			"value":  float64(i % 1000),
			"active": i%2 == 0,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := df.Filter(mframe.Equals, "name", "name_50", nil)
		_ = result.Count()
	}
}

func BenchmarkFilterRegex(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	// Insert test data
	for i := 0; i < 10000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":    i,
			"email": fmt.Sprintf("user%d@example.com", i),
			"name":  fmt.Sprintf("User Number %d", i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := df.Filter(mframe.RegExp, "email", "user[0-9]+@example\\.com", nil)
		_ = result.Count()
	}
}

func BenchmarkFilterNumericRange(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	// Insert test data
	for i := 0; i < 10000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":    i,
			"score": rand.Float64() * 100,
			"age":   rand.Intn(100),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := df.Filter(mframe.Between, "score", []float64{25.0, 75.0}, nil)
		_ = result.Count()
	}
}

func BenchmarkMathOperations(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	// Insert test data
	for i := 0; i < 10000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"value": rand.Float64() * 1000,
		})
	}

	operations := []struct {
		name string
		fn   func() (float64, error)
	}{
		{"Sum", func() (float64, error) { return df.Sum("value") }},
		{"Average", func() (float64, error) { return df.Average("value") }},
		{"Min", func() (float64, error) { return df.Min("value") }},
		{"Max", func() (float64, error) { return df.Max("value") }},
		{"Median", func() (float64, error) { return df.Median("value") }},
	}

	for _, op := range operations {
		b.Run(op.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = op.fn()
			}
		})
	}
}

func BenchmarkConcurrentInsert(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	b.RunParallel(func(pb *testing.PB) {
		data := map[mframe.KeyName]interface{}{
			"thread": fmt.Sprintf("%d", rand.Int()),
			"value":  rand.Float64(),
			"time":   time.Now(),
		}
		for pb.Next() {
			df.Insert(data)
		}
	})
}

func BenchmarkConcurrentFilter(b *testing.B) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)
	df.StartCleaner()
	defer df.StopCleaner()

	// Insert test data
	for i := 0; i < 10000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"category": fmt.Sprintf("cat_%d", i%10),
			"value":    float64(i),
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		cat := fmt.Sprintf("cat_%d", rand.Intn(10))
		for pb.Next() {
			result := df.Filter(mframe.Equals, "category", cat, nil)
			_ = result.Count()
		}
	})
}
