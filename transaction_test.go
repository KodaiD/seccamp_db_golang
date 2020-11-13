package main

import (
	rwuMutex "github.com/KodaiD/rwumutex"
	"testing"
)

func setupForTest() *Tx {
	db := NewTestDB()
	return NewTx(1, db)
}

func TestPattern1(t *testing.T) {
	tx := setupForTest()
	if err := tx.Insert("key1", "value1"); err != nil {
		t.Errorf("failed to insert: %v", err)
	}
	if err := tx.Read("key1"); err != nil {
		t.Errorf("failed to read: %v", err)
	}
	if err := tx.Insert("key2", "value2"); err != nil {
		t.Errorf("failed to insert: %v", err)
	}
	if err := tx.Update("key1", "new_value1"); err != nil {
		t.Errorf("failed to update: %v", err)
	}
	if err := tx.Insert("key3", "value3"); err != nil {
		t.Errorf("failed to insert: %v", err)
	}
	if err := tx.Delete("key2"); err != nil {
		t.Errorf("failed to delete: %v", err)
	}
	tx.Commit()
	if err := tx.Insert("key4", "value4"); err != nil {
		t.Errorf("failed to insert: %v", err)
	}
	newTx := setupForTest()
	if err := newTx.Read("key4"); err == nil {
		t.Error("data after commit exists")
	}
}

func TestTx_Read(t *testing.T) {
	tx := setupForTest()

	// record in read-set
	tx.readSet["test_read"] = &Record{
		key:     "test_read",
		value:   "ans",
		mu:      new(rwuMutex.RWUMutex),
		deleted: false,
	}
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in read-set: %v", err)
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
		t.Errorf("failed to read data in write-set: %v", err)
	}

	// record in Index
	tx.writeSet = make(WriteSet)
	tx.db.index["test_read"] = Record{"test_read", "ans", new(rwuMutex.RWUMutex), false}
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in Index: %v", err)
	}
	tx.DestructTx()
}

func TestTx_Insert(t *testing.T) {
	tx := setupForTest()

	if err := tx.Insert("test_insert", "ans"); err != nil {
		t.Errorf("failed to insert data: %v", err)
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
	tx := setupForTest()
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
		t.Errorf("failed to update data: %v", err)
	}
	if tx.writeSet["test_update"].record.value != "new_ans" {
		t.Error("failed to update (wrong value)")
	}
	tx.DestructTx()
}

func TestTx_Delete(t *testing.T) {
	tx := setupForTest()
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
		t.Errorf("failed to delete data: %v", err)
	}
	if len(tx.writeSet) != 1 || tx.writeSet["test_delete"].cmd != DELETE {
		t.Error("failed to delete data")
	}
	tx.DestructTx()
}

func TestTx_Commit(t *testing.T) {
	tx := setupForTest()
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

	if len(tx.db.index) != 2 {
		t.Error("not committed")
	}
	if tx.db.index["test_commit1"].value != "new_ans" {
		t.Error("update log is not committed")
	}
	if tx.db.index["test_commit2"].deleted != true {
		t.Error("update log is not committed")
	}
	if len(tx.writeSet) != 0 {
		t.Error("write-set is not cleared")
	}
	tx.DestructTx()
}