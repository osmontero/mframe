package mframe

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestEncryptAndDecrypt(t *testing.T) {
	var cache DataFrame
	cache.Init(24 * time.Hour)
	go cache.CleanExpired()
	go cache.Stats("agents")
	kvs := map[string]string{
		"001": "jdjsldfjsfk",
		"002": "jkjskdjslkdfsf",
		"004": "jkjskdjslkdfsf",
		"005": "jkjskdjslkdfsf",
		"006": "jkjskdjslkdfsf",
		"007": "jkjskdjslkdfsf",
		"008": "jkjskdjslkdfsf",
		"009": "jkjskdjslkdfsf",
		"010": "jkjskdjslkdfsf",
		"011": "jkjskdjslkdfsf",
		"012": "jkjskdjslkdfsf",
	}
	for k, v := range kvs {
		cache.Insert(
			map[string]interface{}{
				k: v,
			},
		)
	}

	id, key, value := cache.FindFirstByKey("001")
	if value != "jdjsldfjsfk" {
		t.Error("value is not correct")
	}
	t.Log(id, key, value)
	cache.RemoveElement(id)
	id, key, value = cache.FindFirstByKey("001")
	if id != uuid.Nil {
		t.Error("delete function is not working")
	}
}
