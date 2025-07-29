package mframe_test

import (
	"testing"
	"time"

	"github.com/threatwinds/mframe"
)

func TestExplain(t *testing.T) {
	df := &mframe.DataFrame{}
	df.Init(5 * time.Minute)

	// Insert test data
	for i := 0; i < 1000; i++ {
		df.Insert(map[mframe.KeyName]interface{}{
			"id":      i,
			"name":    "test" + string(rune('A'+i%26)),
			"score":   float64(i % 100),
			"active":  i%2 == 0,
			"created": time.Now().Add(time.Duration(i) * time.Hour),
		})
	}

	tests := []struct {
		name     string
		operator mframe.Operator
		key      mframe.KeyName
		value    interface{}
	}{
		{
			name:     "Numeric Equals",
			operator: mframe.Equals,
			key:      "score",
			value:    50.0,
		},
		{
			name:     "String Equals",
			operator: mframe.Equals,
			key:      "name",
			value:    "testA",
		},
		{
			name:     "Boolean Equals",
			operator: mframe.Equals,
			key:      "active",
			value:    true,
		},
		{
			name:     "Numeric Range",
			operator: mframe.Between,
			key:      "score",
			value:    []float64{25.0, 75.0},
		},
		{
			name:     "Unknown Key",
			operator: mframe.Equals,
			key:      "nonexistent",
			value:    "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := df.Explain(tt.operator, tt.key, tt.value)

			// Verify basic fields
			if result.Operator == "" {
				t.Error("Operator should not be empty")
			}
			if result.Key != string(tt.key) {
				t.Errorf("Key mismatch: got %s, want %s", result.Key, tt.key)
			}
			if result.TotalRows != 1000 {
				t.Errorf("TotalRows mismatch: got %d, want 1000", result.TotalRows)
			}

			// Print the explain output for manual verification
			t.Log(result.String())
		})
	}
}
