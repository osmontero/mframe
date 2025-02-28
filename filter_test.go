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
				{"id": 3, "name": "John", "age": 35},
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
