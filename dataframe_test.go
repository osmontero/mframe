package mframe

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestInsertFindAndDelete(t *testing.T) {
	var cache DataFrame
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
