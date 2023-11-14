package mframe_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/threatwinds/mframe"
)

func TestDataFrame_Init(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	if cache.Data == nil {
		t.Error("Data map is not initialized")
	}

	if cache.Keys == nil {
		t.Error("Keys map is not initialized")
	}

	if cache.Strings == nil {
		t.Error("Strings map is not initialized")
	}

	if cache.Numerics == nil {
		t.Error("Numerics map is not initialized")
	}

	if cache.Booleans == nil {
		t.Error("Booleans map is not initialized")
	}

	if cache.ExpireAt == nil {
		t.Error("ExpireAt map is not initialized")
	}

	if cache.TTL != 24*time.Hour {
		t.Error("TTL is not set correctly")
	}
}

func TestInsertFindAndDelete(t *testing.T) {
	var cache mframe.DataFrame

	cache.Init(24 * time.Hour)

	go cache.CleanExpired()

	go cache.Stats("agents")

	kvs := map[string]string{
		"001": "value1",
		"002": "value2",
		"004": "value3",
		"005": "value4",
		"006": "value5",
		"007": "value6",
		"008": "value7",
		"009": "value8",
		"010": "value9",
		"011": "value10",
		"012": "value11",
	}

	for k, v := range kvs {
		cache.Insert(
			map[string]interface{}{
				k: v,
			},
		)
	}

	id, key, value := cache.FindFirstByKey("001")
	if value != "value1" {
		t.Error("value is not correct")
	}

	t.Log(id, key, value)

	cache.RemoveElement(id)

	id, _, _ = cache.FindFirstByKey("001")
	if id != uuid.Nil {
		t.Error("delete function is not working")
	}
}

func TestToSlice(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := map[string]string{
		"001": "value1",
		"002": "value2",
		"004": "value3",
		"005": "value4",
		"006": "value5",
		"007": "value6",
		"008": "value7",
		"009": "value8",
		"010": "value9",
		"011": "value10",
		"012": "value11",
	}

	for k, v := range kvs {
		cache.Insert(
			map[string]interface{}{
				k: v,
			},
		)
	}

	slice := cache.ToSlice()

	if len(slice) != len(kvs) {
		t.Errorf("Expected slice length %d, but got %d", len(kvs), len(slice))
	}

	for _, row := range slice {
		if len(row) != 1 {
			t.Errorf("Expected row length 1, but got %d", len(row))
		}

		for k, v := range row {
			if kvs[k] != v {
				t.Errorf("Expected value %s for key %s, but got %v", kvs[k], k, v)
			}
		}
	}
}

func TestSliceOf(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := map[string]string{
		"001": "value1",
		"002": "value2",
		"004": "value3",
		"005": "value4",
		"006": "value5",
		"007": "value6",
		"008": "value7",
		"009": "value8",
		"010": "value9",
		"011": "value10",
		"012": "value11",
	}

	for k, v := range kvs {
		cache.Insert(
			map[string]interface{}{
				k: v,
			},
		)
	}

	list := cache.SliceOf("001")

	expectedList := []interface{}{"value1"}

	if !reflect.DeepEqual(list, expectedList) {
		t.Errorf("Expected list %v, but got %v", expectedList, list)
	}

	list = cache.SliceOf("invalid_field")

	expectedList = nil

	if !reflect.DeepEqual(list, expectedList) {
		t.Errorf("Expected list %v, but got %v", expectedList, list)
	}
}

func TestSliceOfFloat64(t *testing.T) {
	var cache mframe.DataFrame
	cache.Init(24 * time.Hour)

	kvs := map[string]float64{
		"001": 1.0,
		"002": 2.0,
		"004": 3.0,
		"005": 4.0,
		"006": 5.0,
		"007": 6.0,
		"008": 7.0,
		"009": 8.0,
		"010": 9.0,
		"011": 10.0,
		"012": 11.0,
	}

	for k, v := range kvs {
		cache.Insert(
			map[string]interface{}{
				k: v,
			},
		)
	}

	list := cache.SliceOfFloat64("001")

	expectedList := []float64{1.0}

	if !reflect.DeepEqual(list, expectedList) {
		t.Errorf("Expected list %v, but got %v", expectedList, list)
	}

	list = cache.SliceOfFloat64("invalid_field")

	expectedList = []float64{}

	if !reflect.DeepEqual(list, expectedList) {
		t.Errorf("Expected list %v, but got %v", expectedList, list)
	}
}
