package main

import (
	rwuMutex "github.com/KodaiD/rwumutex"
	"sync"
	"testing"
	"time"
)

// 1 tx
func TestPattern1(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(1, db)

	if err := tx.Insert("key1", "value1"); err != nil {
		t.Errorf("failed to insert: %v\n", err)
	}
	if err := tx.Read("key1"); err != nil {
		t.Errorf("failed to read: %v\n", err)
	}
	if err := tx.Insert("key2", "value2"); err != nil {
		t.Errorf("failed to insert: %v\n", err)
	}
	if err := tx.Update("key1", "new_value1"); err != nil {
		t.Errorf("failed to update: %v\n", err)
	}
	if err := tx.Insert("key3", "value3"); err != nil {
		t.Errorf("failed to insert: %v\n", err)
	}
	if err := tx.Delete("key2"); err != nil {
		t.Errorf("failed to delete: %v\n", err)
	}
	tx.Commit()
	if err := tx.Insert("key4", "value4"); err != nil {
		t.Errorf("failed to insert: %v\n", err)
	}
	newTx := NewTx(2, db)
	if err := newTx.Read("key4"); err == nil {
		t.Error("data after commit exists")
	}
}

// 2 tx concurrent
func TestPattern2(t *testing.T) {
	db := NewTestDB()
	db.index.Store("key1", Record{"key1", "value1", new(rwuMutex.RWUMutex), false})
	db.index.Store("key2", Record{"key2", "value2", new(rwuMutex.RWUMutex), false})

	tx1 := NewTx(1, db)
	tx2 := NewTx(2, db)

	if err := tx1.Read("key1"); err != nil {
		t.Fatalf("failed to read: %v\n", err)
	}
	if err := tx2.Read("key1"); err != nil {
		t.Fatalf("cannot get read lock: %v\n", err)
	}
	if err := tx1.Insert("key3", "value3"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	if err := tx2.Read("key3"); err == nil {
		t.Fatal("write lock exist, should be failed")
	}
	if err := tx1.Update("key2", "new_value2"); err != nil {
		t.Fatalf("failed to update: %v\n", err)
	}
	if err := tx2.Update("key2", "new_new_value"); err == nil {
		t.Fatal("write lock exist, should be failed")
	}
}

// 2 tx parallel
func TestPattern3(t *testing.T) {
	db := NewTestDB()
	db.index.Store("key1", Record{"key1", "value1", new(rwuMutex.RWUMutex), false})
	db.index.Store("key2", Record{"key2", "value2", new(rwuMutex.RWUMutex), false})
	db.index.Store("key3", Record{"key3", "value3", new(rwuMutex.RWUMutex), false})

	tx1 := NewTx(1, db)
	tx2 := NewTx(2, db)

	wg := sync.WaitGroup{}

	// tx1
	wg.Add(1)
	go func() {
		defer wg.Done()
		tx1.Read("key1")
		tx1.Insert("key4", "value4")
		time.Sleep(time.Second * 3)
		tx1.Commit()
	}()

	// tx2
	wg.Add(1)
	go func() {
		defer wg.Done()
		tx2.Read("key1")
		tx2.Update("key2", "new_value2")
		tx2.Delete("key3")
		time.Sleep(time.Second * 3)
		tx2.Commit()
	}()

	wg.Wait()

	if r, exist := db.index.Load("key1"); exist && r.(Record).value != "value1" {
		t.Fatalf("wrong result: %v\n", r.(Record).value)
	}
	if r, exist := db.index.Load("key2"); exist && r.(Record).value != "new_value2" {
		t.Fatalf("wrong result: %v\n", r.(Record).value)
	}
	if r, exist := db.index.Load("key3"); exist && r.(Record).value != "" {
		t.Fatalf("wrong result: %v\n", r.(Record).value)
	}
	if r, exist := db.index.Load("key4"); exist && r.(Record).value != "value4" {
		t.Fatalf("wrong result: %v\n", r.(Record).value)
	}
}

func TestLogicalDelete(t *testing.T) {
	db := NewTestDB()
	db.index.Store("key1", Record{"key1", "value1", new(rwuMutex.RWUMutex), false})

	tx1 := NewTx(1, db)
	tx2 := NewTx(2, db)

	if err := tx1.Delete("key1"); err != nil {
		t.Fatalf("failed to delete: %v\n", err)
	}
	if err := tx2.Read("key1"); err == nil {
		t.Fatal("write lock exist, should be failed")
	}
	tx1.Commit()
	if record, exist := db.index.Load("key1"); exist && !record.(Record).deleted {
		t.Fatal("logical delete failed")
	}

	if err := tx1.Read("key2"); err == nil {
		t.Fatal("should be failed")
	}
	if err := tx2.Insert("key2", "value2"); err == nil {
		t.Fatal("should be failed")
	}
}

func TestTx_Read(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(1, db)

	// record in read-set
	tx.readSet["test_read"] = &Record{
		key:     "test_read",
		value:   "ans",
		mu:      new(rwuMutex.RWUMutex),
		deleted: false,
	}
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in read-set: %v\n", err)
	}

	// record in write-set
	tx.writeSet["test_read"] = &Operation{
		cmd: INSERT,
		record: &Record{
			key:     "test_read",
			value:   "ans",
			mu:      new(rwuMutex.RWUMutex),
			deleted: false,
		},
	}
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in write-set: %v\n", err)
	}

	// record in Index
	tx.writeSet = make(WriteSet)
	tx.db.index.Store("test_read", Record{"test_read", "ans", new(rwuMutex.RWUMutex), false})
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in Index: %v\n", err)
	}
	tx.DestructTx()
}

func TestTx_Insert(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(1, db)

	if err := tx.Insert("test_insert", "ans"); err != nil {
		t.Errorf("failed to insert data: %v\n", err)
	}
	op, exist := tx.writeSet["test_insert"]
	if !exist {
		t.Error("not exist")
	}
	if op.record.value != "ans" {
		t.Error("failed to insert data (wrong value)")
	}
	tx.DestructTx()
}

func TestTx_Update(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(1, db)
	tx.writeSet["test_update"] = &Operation{
		cmd: INSERT,
		record: &Record{
			key:     "test_update",
			value:   "ans",
			mu:      new(rwuMutex.RWUMutex),
			deleted: false,
		},
	}
	if err := tx.Update("test_update", "new_ans"); err != nil {
		t.Errorf("failed to update data: %v\n", err)
	}
	if tx.writeSet["test_update"].record.value != "new_ans" {
		t.Error("failed to update (wrong value)")
	}
	tx.DestructTx()
}

func TestTx_Delete(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(1, db)
	tx.writeSet["test_delete"] = &Operation{
		cmd: INSERT,
		record: &Record{
			key:     "test_delete",
			value:   "ans",
			mu:      new(rwuMutex.RWUMutex),
			deleted: false,
		},
	}
	if err := tx.Delete("test_delete"); err != nil {
		t.Errorf("failed to delete data: %v\n", err)
	}
	if len(tx.writeSet) != 1 || tx.writeSet["test_delete"].cmd != DELETE {
		t.Error("failed to delete data")
	}
	tx.DestructTx()
}

func TestTx_Commit(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(1, db)
	tx.writeSet["test_commit1"] = &Operation{
		cmd: UPDATE,
		record: &Record{
			key:     "test_commit1",
			value:   "new_ans",
			mu:      new(rwuMutex.RWUMutex),
			deleted: false,
		},
	}
	tx.writeSet["test_commit2"] = &Operation{
		cmd: DELETE,
		record: &Record{
			key:     "test_commit2",
			value:   "",
			mu:      new(rwuMutex.RWUMutex),
			deleted: false,
		},
	}

	tx.Commit()

	if record, exist := tx.db.index.Load("test_commit1"); exist && record.(Record).value != "new_ans" {
		t.Error("update log is not committed")
	}
	if record, exist := tx.db.index.Load("test_commit2"); exist && record.(Record).deleted != true {
		t.Error("update log is not committed")
	}
	if len(tx.writeSet) != 0 {
		t.Error("write-set is not cleared")
	}
	tx.DestructTx()
}