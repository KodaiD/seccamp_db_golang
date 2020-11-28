package main

import (
	"sync"
	"testing"
	"time"
)

// 1 tx
func TestPattern1(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(db)
	if err := tx.Insert("key1", "value1"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	if value, err := tx.Read("key1"); err != nil || value != "value1" {
		t.Fatalf("failed to read: %v\n", err)
	}
	if err := tx.Insert("key2", "value2"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	if err := tx.Update("key1", "new_value1"); err != nil {
		t.Fatalf("failed to update: %v\n", err)
	}
	if err := tx.Insert("key3", "value3"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	if err := tx.Delete("key2"); err != nil {
		t.Fatalf("failed to delete: %v\n", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	tx.DestructTx()
	tx = NewTx(db)
	if err := tx.Insert("key4", "value4"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	newTx := NewTx(db)
	if _, err := newTx.Read("key4"); err == nil {
		t.Fatal("data after commit exists")
	}
}

// 2 tx concurrent
func TestPattern2(t *testing.T) {
	db := NewTestDB()

	v1 := &Version{
		key:     "key1",
		value:   "value1",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	v2 := &Version{
		key:     "key2",
		value:   "value2",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	db.index.Store("key1", &Record{
		key:   "key1",
		first: v1,
		last:  v1,
		mu:    sync.Mutex{},
	})
	db.index.Store("key2", &Record{
		key:   "key2",
		first: v2,
		last:  v2,
		mu:    sync.Mutex{},
	})

	tx1 := NewTx(db)
	tx2 := NewTx(db)

	// tx1: r1(1)       i1(3)       u1(2) /d1(1)/             c1(fail)
	// tx2:       /r2(1)/       r2(3)             c2(success)

	if value, err := tx1.Read("key1"); err != nil || value != "value1" {
		t.Fatalf("failed to read: %v\n", err)
	}
	if value, err := tx2.Read("key1"); err != nil || value != "value1" {
		t.Fatalf("failed to read: %v\n", err)
	}
	if err := tx1.Insert("key3", "value3"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	if _, err := tx2.Read("key3"); err == nil {
		t.Fatal("should be failed")
	}
	if err := tx1.Update("key2", "new_value2"); err != nil {
		t.Fatalf("failed to update: %v\n", err)
	}
	if err := tx1.Delete("key1"); err != nil {
		t.Errorf("failed to delete: %v", err)
	}

	if err := tx2.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	if err := tx1.Commit(); err == nil {
		t.Fatal("should be failed")
	}
	tx1.DestructTx()
	tx2.DestructTx()
	tx1 = NewTx(db)

	// tx1: d1(1) c1(success)

	if err := tx1.Delete("key1"); err != nil {
		t.Errorf("failed to delete: %v", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx1.DestructTx()
	tx3 := NewTx(db)

	if value, err := tx3.Read("key1"); err == nil {
		t.Fatalf("should be failed: read %v" ,value)
	}
	if _, err := tx3.Read("key1"); err == nil {
		t.Fatal("should be failed")
	}
	if value, err := tx3.Read("key2"); err != nil || value != "value2" {
		t.Fatalf("failed to read: [ERROR] %v [VALUE], \n", err)
	}
	if value, err := tx3.Read("key2"); err != nil || value != "value2" {
		t.Fatalf("failed to read: [ERROR] %v [VALUE], \n", err)
	}
	if value, err := tx3.Read("key3"); err == nil {
		t.Fatalf("failed to read: %v returned\n", value)
	}
	if value, err := tx3.Read("key3"); err == nil {
		t.Fatalf("failed to read: %v returned\n", value)
	}
}

// 2 tx parallel
func TestPattern3(t *testing.T) {
	db := NewTestDB()

	v1 := &Version{
		key:     "key1",
		value:   "value1",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	v2 := &Version{
		key:     "key2",
		value:   "value2",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	v3 := &Version{
		key:     "key3",
		value:   "value3",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	db.index.Store("key1", &Record{
		key:   "key1",
		first: v1,
		last:  v1,
		mu:    sync.Mutex{},
	})
	db.index.Store("key2", &Record{
		key:   "key2",
		first: v2,
		last:  v2,
		mu:    sync.Mutex{},
	})
	db.index.Store("key3", &Record{
		key:   "key3",
		first: v3,
		last:  v3,
		mu:    sync.Mutex{},
	})

	tx1 := NewTx(db)
	tx2 := NewTx(db)

	wg := sync.WaitGroup{}

	// tx1
	wg.Add(1)
	go func() {
		defer wg.Done()
		if value, err := tx1.Read("key1"); err != nil || value != "value1" {
			t.Fatalf("failed to read: %v", err)
		}
		if err := tx1.Insert("key4", "value4"); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		time.Sleep(time.Second * 3)
		if err := tx1.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
		tx1.DestructTx()
		tx1 = NewTx(db)
	}()

	// tx2
	wg.Add(1)
	go func() {
		defer wg.Done()
		if value, err := tx2.Read("key1"); err != nil || value != "value1" {
			t.Fatalf("failed to read: %v", err)
		}
		if err := tx2.Update("key2", "new_value2"); err != nil {
			t.Fatalf("failed to update: %v", err)
		}
		if err := tx2.Delete("key3"); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
		time.Sleep(time.Second * 3)
		if err := tx2.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
		tx2.DestructTx()
		tx2 = NewTx(db)
	}()

	wg.Wait()

	if v, exist := db.index.Load("key1"); !exist || v.(*Record).last.value != "value1" {
		t.Fatalf("wrong result: %v", v.(Record).last.value)
	}
	if v, exist := db.index.Load("key2"); !exist || v.(*Record).last.value != "new_value2" {
		t.Fatalf("wrong result: %v", v.(Record).last.value)
	}
	if v, exist := db.index.Load("key3"); !exist || !(v.(*Record).last.value == "" && v.(*Record).last.deleted) {
		t.Fatalf("wrong result: %v, should be deleted", v.(Record).last.value)
	}
	if v, exist := db.index.Load("key4"); !exist || v.(*Record).last.value != "value4" {
		t.Fatalf("wrong result: %v", v.(Record).last.value)
	}

	// tx1: r(1) i(4)      c(success)
	// tx2: r(1) u(2) d(3)      c(success)
	// r1(1) r2(1) u2(2) d2(3) i1(4) c1 c2

	// ========================================================================================

	// tx1
	if value, err := tx1.Read("key1"); err != nil || value != "value1" {
		t.Fatalf("failed to read: %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tx1.Insert("key6", "value6"); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		time.Sleep(time.Second * 3)
		if err := tx1.Commit(); err != nil {
			t.Fatalf("failed to commit: %v", err)
		}
		tx1.DestructTx()
	}()

	// tx2
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tx2.Update("key1", "new_value1"); err != nil {
			t.Fatalf("failed to update: %v", err)
		}
		if err := tx2.Update("key2", "new_new_value2"); err != nil {
			t.Fatalf("failed to update: %v", err)
		}
		if err := tx2.Delete("key4"); err != nil {
			t.Fatalf("failed to delete: %v", err)
		}
		if err := tx2.Insert("key5", "value5"); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		time.Sleep(time.Second * 3)
		tx2.Abort()
		tx2.DestructTx()
	}()

	wg.Wait()

	if v, exist := db.index.Load("key1"); !exist || v.(*Record).last.value != "value1" {
		t.Fatalf("wrong result: %v", v.(*Record).last.value)
	}
	if v, exist := db.index.Load("key2"); !exist || v.(*Record).last.value != "new_value2" {
		t.Fatalf("wrong result: %v", v.(*Record).last.value)
	}
	if v, exist := db.index.Load("key3"); !exist || !(v.(*Record).last.value == "" && v.(*Record).last.deleted) {
		t.Fatalf("wrong result: %v, should be deleted", v.(*Record).last.value)
	}
	if v, exist := db.index.Load("key4"); !exist || v.(*Record).last.value != "value4" {
		t.Fatalf("wrong result: %v", v.(*Record).last.value)
	}
	if v, exist := db.index.Load("key5"); exist {
		t.Fatalf("wrong result: %v, should not be deleted", v.(*Record).last.value)
	}
	if v, exist := db.index.Load("key6"); !exist || v.(*Record).last.value != "value6" {
		t.Fatalf("wrong result: %v", v.(*Record).last.value)
	}

	// tx1: r(1) i(6)     c(success)
	// tx2: u(1) u(2) d(4) i(5) a
	// r1(1) u2(1) i1(6) u2(2) d2(4) c1 i2(5) c2
}

func TestLogicalDelete(t *testing.T) {
	db := NewTestDB()
	v1 := &Version{
		key:     "key1",
		value:   "value1",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	db.index.Store("key1", &Record{
		key:   "key1",
		first: v1,
		last:  v1,
		mu:    sync.Mutex{},
	})
	tx1 := NewTx(db)
	tx2 := NewTx(db)

	if err := tx1.Delete("key1"); err != nil {
		t.Fatalf("failed to delete: %v\n", err)
	}
	if err := tx2.Delete("key1"); err != nil {
		t.Fatalf("failed to delete: %v\n", err)
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
	tx1.DestructTx()
	tx1 = NewTx(db)

	if record, exist := db.index.Load("key1"); !exist || !record.(*Record).last.deleted {
		t.Fatal("logical delete failed")
	}
	if _, err := tx1.Read("key1"); err == nil {
		t.Fatal("should be failed")
	}
	if err := tx2.Insert("key1", "value1"); err != nil {
		t.Fatalf("failed to insert: %v\n", err)
	}
	if err := tx2.Commit(); err == nil {
		t.Fatal("should be failed")
	}
}

func TestTx_Read(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(db)

	// record in read-set
	tx.readSet["test_read"] = &Version{
		key:     "test_read",
		value:   "ans",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	if value, err := tx.Read("test_read"); err != nil || value != "ans" {
		t.Errorf("failed to read data in read-set: %v\n", err)
	}

	// record in write-set
	tx.writeSet["test_read"] = append(tx.writeSet["test_read"], &Operation{
		cmd: INSERT,
		version: &Version{
			key:     "test_read",
			value:   "ans",
			wTs:     0,
			rTs:     0,
			prev:    nil,
			deleted: false,
		},
	})
	if value, err := tx.Read("test_read"); err != nil || value != "ans" {
		t.Errorf("failed to read data in write-set: %v\n", err)
	}

	// record in Index
	tx.readSet = make(ReadSet)
	tx.writeSet = make(WriteSet)
	v := &Version{
		key:     "test_read",
		value:   "ans",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	tx.db.index.Store("test_read", &Record{
		key:   "test_read",
		first: v,
		last:  v,
		mu:    sync.Mutex{},
	})
	if value, err := tx.Read("test_read"); err != nil || value != "ans" {
		t.Errorf("failed to read data in Index: %v\n", err)
	}
	tx.DestructTx()
}

func TestTx_Insert(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(db)

	if err := tx.Insert("test_insert", "ans"); err != nil {
		t.Errorf("failed to insert data: %v\n", err)
	}
	operations, exist := tx.writeSet["test_insert"]
	op := operations[0]
	if !exist {
		t.Error("not exist")
	}
	if op.version.value != "ans" {
		t.Error("failed to insert data (wrong value)")
	}
	tx.DestructTx()
}

func TestTx_Update(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(db)
	tx.writeSet["test_update"] = append(tx.writeSet["test_update"], &Operation{
		cmd: INSERT,
		version: &Version{
			key:     "test_update",
			value:   "ans",
			wTs:     0,
			rTs:     0,
			prev:    nil,
			deleted: false,
		},
	})
	if err := tx.Update("test_update", "new_ans"); err != nil {
		t.Errorf("failed to update data: %v\n", err)
	}
	if value := tx.writeSet["test_update"][1].version.value; value != "new_ans" {
		t.Errorf("failed to update (wrong value: %v)", value)
	}
	tx.DestructTx()
}

func TestTx_Delete(t *testing.T) {
	db := NewTestDB()
	tx := NewTx(db)
	tx.writeSet["test_delete"] = append(tx.writeSet["test_delete"], &Operation{
		cmd: INSERT,
		version: &Version{
			key:     "test_update",
			value:   "ans",
			wTs:     0,
			rTs:     0,
			prev:    nil,
			deleted: false,
		},
	})
	if err := tx.Delete("test_delete"); err != nil {
		t.Errorf("failed to delete data: %v\n", err)
	}
	if len(tx.writeSet) != 1 || tx.writeSet["test_delete"][1].cmd != DELETE {
		t.Error("failed to delete data")
	}
	tx.DestructTx()
}

func TestTx_Commit(t *testing.T) {
	db := NewTestDB()
	v1 := &Version{
		key:     "test_commit1",
		value:   "ans1",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	v2 := &Version{
		key:     "test_commit2",
		value:   "ans2",
		wTs:     0,
		rTs:     0,
		prev:    nil,
		deleted: false,
	}
	db.index.Store("test_commit1", &Record{
		key:   "test_commit1",
		first: v1,
		last:  v1,
		mu:    sync.Mutex{},
	})
	db.index.Store("test_commit2", &Record{
		key:   "test_commit2",
		first: v2,
		last:  v2,
		mu:    sync.Mutex{},
	})

	tx := NewTx(db)
	tx.writeSet["test_commit1"] = append(tx.writeSet["test_commit1"], &Operation{
		cmd: UPDATE,
		version: &Version{
			key:     "test_commit1",
			value:   "new_ans",
			wTs:     0,
			rTs:     0,
			prev:    nil,
			deleted: false,
		},
	})
	tx.writeSet["test_commit2"] = append(tx.writeSet["test_commit2"], &Operation{
		cmd: DELETE,
		version: &Version{
			key:     "test_commit2",
			value:   "",
			wTs:     0,
			rTs:     0,
			prev:    nil,
			deleted: false,
		},
	})

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	tx.DestructTx()
	tx = NewTx(db)

	if record, exist := tx.db.index.Load("test_commit1"); exist && record.(*Record).last.value != "new_ans" {
		t.Errorf("update log is not committed: %v", record.(Record).last.value)
	}
	if record, exist := tx.db.index.Load("test_commit2"); exist && record.(*Record).last.value != "" {
		t.Errorf("delete log is not committed: %v", record.(Record).last.value)
	}
	if len(tx.writeSet) != 0 {
		t.Error("write-set is not cleared")
	}
	tx.DestructTx()
}
