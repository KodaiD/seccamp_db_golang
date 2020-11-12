package main

//func TestPattern1(t *testing.T) {
//	index := make(Index)
//	tx := setupForTest(index)
//	if err := tx.Insert("key1", "value1"); err != nil {
//		t.Errorf("failed to insert: %v", err)
//	}
//	if err := tx.Read("key1"); err != nil {
//		t.Errorf("failed to read: %v", err)
//	}
//	if err := tx.Insert("key2", "value2"); err != nil {
//		t.Errorf("failed to insert: %v", err)
//	}
//	if err := tx.Update("key1", "new_value1"); err != nil {
//		t.Errorf("failed to update: %v", err)
//	}
//	if err := tx.Insert("key3", "value3"); err != nil {
//		t.Errorf("failed to insert: %v", err)
//	}
//	if err := tx.Delete("key2"); err != nil {
//		t.Errorf("failed to delete: %v", err)
//	}
//	tx.Commit()
//	if err := tx.Insert("key4", "value4"); err != nil {
//		t.Errorf("failed to insert: %v", err)
//	}
//	newTx := setupForTest(index)
//	if err := newTx.Read("key4"); err == nil {
//		t.Error("data after commit exists")
//	}
//}

//func TestTx_Read(t *testing.T) {
//	db := NewDB(TestWALFileName, TestDBFileName)
//
//
//
//	// =========================================
//	index := make(Index)
//	tx := setupForTest(index)
//
//	// record in write-set
//	tx.writeSet["test_read"] = &Operation{
//		cmd: INSERT,
//		Record: Record{
//			key:   "test_read",
//			value: "ans",
//	}
//	tx.writeSet = append(tx.writeSet, Operation{
//		cmd: INSERT,
//		Record: Record{
//			key:   "test_read",
//			value: "ans",
//		},
//	})
//	if err := tx.Read("test_read"); err != nil {
//		t.Errorf("failed to read data in write-set: %v", err)
//	}
//
//	// record in index
//	tx.writeSet = WriteSet{}
//	tx.db.index["test_read"] = Record{"test_read", "ans", new(rwuMutex.RWUMutex), false}
//	if err := tx.Read("test_read"); err != nil {
//		t.Errorf("failed to read data in index: %v", err)
//	}
//	tx.DestructTx()
//}
//
//func TestTx_Insert(t *testing.T) {
//	index := make(Index)
//	tx := setupForTest(index)
//
//	if err := tx.Insert("test_insert", "ans"); err != nil {
//		t.Errorf("failed to insert data: %v", err)
//	}
//	if tx.writeSet[0].Record.key != "test_insert" {
//		t.Error("failed to insert data (wrong key)")
//	}
//	if tx.writeSet[0].Record.value != "ans" {
//		t.Error("failed to insert data (wrong value)")
//	}
//	tx.DestructTx()
//}
//
//func TestTx_Update(t *testing.T) {
//	index := make(Index)
//	tx := setupForTest(index)
//	tx.writeSet = append(tx.writeSet, Operation{
//		cmd: INSERT,
//		Record: Record{
//			key:   "test_update",
//			value: "ans",
//		},
//	})
//	if err := tx.Update("test_update", "new_ans"); err != nil {
//		t.Errorf("failed to update data: %v", err)
//	}
//	if tx.writeSet[1].value != "new_ans" {
//		t.Error("failed to update (wrong value)")
//	}
//	tx.DestructTx()
//}
//
//func TestTx_Delete(t *testing.T) {
//	index := make(Index)
//	tx := setupForTest(index)
//	tx.writeSet = append(tx.writeSet, Operation{
//		cmd: INSERT,
//		Record: Record{
//			key:   "test_delete",
//			value: "ans",
//		},
//	})
//	if err := tx.Delete("test_delete"); err != nil {
//		t.Errorf("failed to delete data: %v", err)
//	}
//	if len(tx.writeSet) != 2 || tx.writeSet[1].cmd != DELETE {
//		t.Error("failed to delete data")
//	}
//	tx.DestructTx()
//}
//
//func TestTx_Commit(t *testing.T) {
//	index := make(Index)
//	tx := setupForTest(index)
//
//	data1 := Operation{
//		cmd: INSERT,
//		Record: Record{
//			key:   "test_commit1",
//			value: "ans1",
//		},
//	}
//	data2 := Operation{
//		cmd: INSERT,
//		Record: Record{
//			key:   "test_commit2",
//			value: "ans2",
//		},
//	}
//	data3 := Operation{
//		cmd: UPDATE,
//		Record: Record{
//			key:   "test_commit1",
//			value: "new_ans",
//		},
//	}
//	data4 := Operation{
//		cmd: DELETE,
//		Record: Record{
//			key: "test_commit2",
//		},
//	}
//	tx.writeSet = []Operation{data1, data2, data3, data4}
//
//	tx.Commit()
//
//	if len(tx.Index) != 1 {
//		t.Error("delete log is not committed")
//	}
//	if tx.Index["test_commit1"] != "new_ans" {
//		t.Error("update log is not committed")
//	}
//	if len(tx.writeSet) != 0 {
//		t.Error("write-set is not cleared")
//	}
//	tx.DestructTx()
//}
//
//func setupForTest(index Index) *Tx {
//	testWalFile, err := os.OpenFile(TestWALFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	return NewTx(1, testWalFile, index)
//}