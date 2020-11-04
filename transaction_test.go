package main

import (
	"log"
	"os"
	"testing"
)

func TestPattern1(t *testing.T) {
	index := make(Index)
	tx := setupForTest(index)
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
	newTx := setupForTest(index)
	if err := newTx.Read("key4"); err == nil {
		t.Error("data after commit exists")
	}
}

func TestTx_Read(t *testing.T) {
	index := make(Index)
	tx := setupForTest(index)

	// record in write-set
	tx.WriteSet = append(tx.WriteSet, Operation{
		CMD:    INSERT,
		Record: Record{
			Key: "test_read",
			Value: "ans",
		},
	})
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in write-set: %v", err)
	}

	// record in Index
	tx.WriteSet = WriteSet{}
	tx.Index["test_read"] = "ans"
	if err := tx.Read("test_read"); err != nil {
		t.Errorf("failed to read data in Index: %v", err)
	}
	tx.DestructTx()
}

func TestTx_Insert(t *testing.T) {
	index := make(Index)
	tx := setupForTest(index)

	if err := tx.Insert("test_insert", "ans"); err != nil {
		t.Errorf("failed to insert data: %v", err)
	}
	if tx.WriteSet[0].Record.Key != "test_insert" {
		t.Error("failed to insert data (wrong key)")
	}
	if tx.WriteSet[0].Record.Value != "ans" {
		t.Error("failed to insert data (wrong value)")
	}
	tx.DestructTx()
}

func TestTx_Update(t *testing.T) {
	index := make(Index)
	tx := setupForTest(index)
	tx.WriteSet = append(tx.WriteSet, Operation{
		CMD:    INSERT,
		Record: Record{
			Key: "test_update",
			Value: "ans",
		},
	})
	if err := tx.Update("test_update", "new_ans"); err != nil {
		t.Errorf("failed to update data: %v", err)
	}
	if tx.WriteSet[1].Value != "new_ans" {
		t.Error("failed to update (wrong value)")
	}
	tx.DestructTx()
}

func TestTx_Delete(t *testing.T) {
	index := make(Index)
	tx := setupForTest(index)
	tx.WriteSet = append(tx.WriteSet, Operation{
		CMD:    INSERT,
		Record: Record{
			Key: "test_delete",
			Value: "ans",
		},
	})
	if err := tx.Delete("test_delete"); err != nil {
		t.Errorf("failed to delete data: %v", err)
	}
	if len(tx.WriteSet) != 2 || tx.WriteSet[1].CMD != DELETE {
		t.Error("failed to delete data")
	}
	tx.DestructTx()
}

func TestTx_Commit(t *testing.T) {
	index := make(Index)
	tx := setupForTest(index)

	data1 := Operation{
		CMD: INSERT,
		Record: Record{
			Key:   "test_commit1",
			Value: "ans1",
		},
	}
	data2 := Operation{
		CMD: INSERT,
		Record: Record{
			Key:   "test_commit2",
			Value: "ans2",
		},
	}
	data3 := Operation{
		CMD: UPDATE,
		Record: Record{
			Key:   "test_commit1",
			Value: "new_ans",
		},
	}
	data4 := Operation{
		CMD: DELETE,
		Record: Record{
			Key: "test_commit2",
		},
	}
	tx.WriteSet = []Operation{data1, data2, data3, data4}

	tx.Commit()

	if len(tx.Index) != 1 {
		t.Error("delete log is not committed")
	}
	if tx.Index["test_commit1"] != "new_ans" {
		t.Error("update log is not committed")
	}
	if len(tx.WriteSet) != 0 {
		t.Error("write-set is not cleared")
	}
	tx.DestructTx()
}

func setupForTest(index Index) *Tx {
	testWalFile, err := os.OpenFile(TestWALFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	return NewTx(1, testWalFile, index)
}