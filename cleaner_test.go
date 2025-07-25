package mframe_test

import (
	"testing"
	"time"

	"github.com/threatwinds/mframe"
)

func TestCleanExpired(t *testing.T) {
	df := mframe.DataFrame{}
	df.Init(3 * time.Second)

	df.StartCleaner()
	defer df.StopCleaner()

	df.Insert(map[mframe.KeyName]interface{}{"id": 1, "name": "John"})
	df.Insert(map[mframe.KeyName]interface{}{"id": 2, "name": "Jane"})

	time.Sleep(5 * time.Second)

	df.Insert(map[mframe.KeyName]interface{}{"id": 3, "name": "Bob"})

	if len(df.Data) != 1 {
		t.Errorf("expected 1 rows, but got %d", len(df.Data))
	}
}
