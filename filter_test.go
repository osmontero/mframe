package mframe_test

import (
	"testing"
	"time"

	"github.com/threatwinds/mframe"
)

func TestFilter(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "name": "John", "age": 25},
		{"id": 2, "name": "Jane", "age": 30},
		{"id": 3, "name": "John", "age": 35},
		{"id": 4, "name": "Jane", "age": 40},
		{"id": 5, "name": "John", "age": 45},
		{"id": 6, "ip": "192.168.1.1"},
		{"id": 7, "ip": "192.168.1.2"},
		{"id": 8, "ip": "192.168.1.3"},
		{"id": 9, "ip": "10.168.1.1"},
		{"id": 10, "ip": "10.168.1.2"},
		{"id": 11, "ip": "10.168.1.3"},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	tests := []struct {
		name     string
		operator mframe.Operator
		key      mframe.KeyName
		value    interface{}
		options  map[mframe.FilterOption]bool
		want     []map[mframe.KeyName]interface{}
	}{
		{
			name:     "Equal",
			operator: mframe.Equals,
			key:      "name",
			value:    "John",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 3, "name": "John", "age": 35},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "NotEqual",
			operator: mframe.NotEquals,
			key:      "name",
			value:    "John",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 4, "name": "Jane", "age": 40},
			},
		},
		{
			name:     "GreaterThan",
			operator: mframe.Major,
			key:      "age",
			value:    35.0,
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "LessThan",
			operator: mframe.Minor,
			key:      "age",
			value:    35.0,
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 2, "name": "Jane", "age": 30},
			},
		},
		{
			name:     "GreaterThanOrEqual",
			operator: mframe.MajorEquals,
			key:      "age",
			value:    35.0,
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 3, "name": "John", "age": 35},
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "LessThanOrEqual",
			operator: mframe.MinorEquals,
			key:      "age",
			value:    35.0,
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
			},
		},
		{
			name:     "InList",
			operator: mframe.InList,
			key:      "age",
			value:    []float64{30.0, 35.0},
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
			},
		},
		{
			name:     "NotInList",
			operator: mframe.NotInList,
			key:      "age",
			value:    []float64{30.0, 35.0},
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "CaseInsensitive",
			operator: mframe.Equals,
			key:      "name",
			value:    "jane",
			options:  map[mframe.FilterOption]bool{mframe.CaseSensitive: false},
			want: []map[mframe.KeyName]interface{}{
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 4, "name": "Jane", "age": 40},
			},
		},
		{
			name:     "RegExp",
			operator: mframe.RegExp,
			key:      "name",
			value:    "^J",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "NotRegExp",
			operator: mframe.NotRegExp,
			key:      "name",
			value:    "^K",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "InCIDR",
			operator: mframe.InCIDR,
			key:      "ip",
			value:    "192.168.1.0/24",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "ip": "192.168.1.1"},
				{"id": 2, "ip": "192.168.1.2"},
				{"id": 3, "ip": "192.168.1.3"},
			},
		},
		{
			name:     "NotInCIDR",
			operator: mframe.NotInCIDR,
			key:      "ip",
			value:    "192.168.1.0/24",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 9, "ip": "10.168.1.1"},
				{"id": 10, "ip": "10.168.1.2"},
				{"id": 11, "ip": "10.168.1.3"},
			},
		},
		{
			name:     "Contains",
			operator: mframe.Contains,
			key:      "name",
			value:    "oh",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 3, "name": "John", "age": 35},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "NotContains",
			operator: mframe.NotContains,
			key:      "name",
			value:    "oh",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 4, "name": "Jane", "age": 40},
			},
		},
		{
			name:     "StartsWith",
			operator: mframe.StartsWith,
			key:      "name",
			value:    "J",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "NotStartsWith",
			operator: mframe.NotStartsWith,
			key:      "name",
			value:    "K",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
				{"id": 4, "name": "Jane", "age": 40},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "EndsWith",
			operator: mframe.EndsWith,
			key:      "name",
			value:    "n",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 3, "name": "John", "age": 35},
				{"id": 5, "name": "John", "age": 45},
			},
		},
		{
			name:     "NotEndsWith",
			operator: mframe.NotEndsWith,
			key:      "name",
			value:    "n",
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 4, "name": "Jane", "age": 40},
			},
		},
		{
			name:     "Between",
			operator: mframe.Between,
			key:      "age",
			value:    []float64{30.0, 40.0},
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 2, "name": "Jane", "age": 30},
				{"id": 3, "name": "John", "age": 35},
				{"id": 4, "name": "Jane", "age": 40},
			},
		},
		{
			name:     "NotBetween",
			operator: mframe.NotBetween,
			key:      "age",
			value:    []float64{30.0, 40.0},
			options:  nil,
			want: []map[mframe.KeyName]interface{}{
				{"id": 1, "name": "John", "age": 25},
				{"id": 5, "name": "John", "age": 45},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cache.Filter(tt.operator, tt.key, tt.value, tt.options)

			if len(got.Data) != len(tt.want) {
				t.Errorf("Expected %d rows, but got %d", len(tt.want), len(got.Data))
			}
		})
	}
}

func TestTimeRangeFilter(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)
	cache.StartCleaner()
	defer cache.StopCleaner()

	// Create test data with time fields
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	kvs := []map[mframe.KeyName]interface{}{
		{"id": 1, "name": "Event1", "created_at": baseTime},
		{"id": 2, "name": "Event2", "created_at": baseTime.Add(24 * time.Hour)},
		{"id": 3, "name": "Event3", "created_at": baseTime.Add(48 * time.Hour)},
		{"id": 4, "name": "Event4", "created_at": baseTime.Add(72 * time.Hour)},
		{"id": 5, "name": "Event5", "created_at": baseTime.Add(96 * time.Hour)},
		{"id": 6, "name": "Event6", "created_at": baseTime.Add(120 * time.Hour)},
	}

	for _, v := range kvs {
		cache.Insert(v)
	}

	tests := []struct {
		name     string
		operator mframe.Operator
		key      mframe.KeyName
		value    interface{}
		wantIDs  []int
	}{
		{
			name:     "TimeBetween_MiddleRange",
			operator: mframe.Between,
			key:      "created_at",
			value: []time.Time{
				baseTime.Add(24 * time.Hour), // Jan 2
				baseTime.Add(72 * time.Hour), // Jan 4
			},
			wantIDs: []int{2, 3, 4}, // Events 2, 3, 4
		},
		{
			name:     "TimeBetween_FullRange",
			operator: mframe.Between,
			key:      "created_at",
			value: []time.Time{
				baseTime,
				baseTime.Add(120 * time.Hour),
			},
			wantIDs: []int{1, 2, 3, 4, 5, 6}, // All events
		},
		{
			name:     "TimeBetween_ReversedOrder",
			operator: mframe.Between,
			key:      "created_at",
			value: []time.Time{
				baseTime.Add(72 * time.Hour), // Jan 4 (end)
				baseTime.Add(24 * time.Hour), // Jan 2 (start) - reversed
			},
			wantIDs: []int{2, 3, 4}, // Should still work correctly
		},
		{
			name:     "TimeNotBetween_MiddleRange",
			operator: mframe.NotBetween,
			key:      "created_at",
			value: []time.Time{
				baseTime.Add(24 * time.Hour), // Jan 2
				baseTime.Add(72 * time.Hour), // Jan 4
			},
			wantIDs: []int{1, 5, 6}, // Events 1, 5, 6
		},
		{
			name:     "TimeNotBetween_ExcludeAll",
			operator: mframe.NotBetween,
			key:      "created_at",
			value: []time.Time{
				baseTime.Add(-24 * time.Hour), // Before all
				baseTime.Add(144 * time.Hour), // After all
			},
			wantIDs: []int{}, // No events outside this range
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cache.Filter(tt.operator, tt.key, tt.value, nil)

			if len(got.Data) != len(tt.wantIDs) {
				t.Errorf("Expected %d rows, but got %d", len(tt.wantIDs), len(got.Data))
			}

			// Check that we got the expected IDs
			gotIDs := make(map[int]bool)
			for _, row := range got.Data {
				// Check for int first
				if id, ok := row["id"].(int); ok {
					gotIDs[id] = true
				} else if idFloat, ok := row["id"].(float64); ok {
					// DataFrame stores numbers as float64
					gotIDs[int(idFloat)] = true
				}
			}

			for _, wantID := range tt.wantIDs {
				if !gotIDs[wantID] {
					t.Errorf("Expected ID %d in results but it was not found", wantID)
				}
			}
		})
	}
}
