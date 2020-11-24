package main

import (
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"sync"
	"testing"
)

const (
	TestDBFileName  = "test_seccampdb.db"
	TestWALFileName = "test_seccampdb.log"
)

func TestDB_LoadData(t *testing.T) {
	generateTestData()
	db := NewTestDB()
	defer db.dBFile.Close()
	defer db.wALFile.Close()

	// crash recovery (db-file -> db-memory)
	db.loadData()
	if record, _ := db.index.Load("test1"); record.(Record).last.value != "value1" {
		t.Error("failed to load data")
	}
}

func TestDB_LoadWal(t *testing.T) {
	db := NewTestDB()
	generateTestData()
	defer db.dBFile.Close()
	defer db.wALFile.Close()

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
	db.index.Store("key1", Record{
		key:   "key1",
		first: v1,
		last:  v1,
	})
	db.index.Store("key2", Record{
		key:   "key2",
		first: v2,
		last:  v2,
	})
	db.index.Store("key3", Record{
		key:   "key3",
		first: v3,
		last:  v3,
	})

	// crash recovery (wal-file -> db-memory)
	db.loadWal()
	if _, exist := db.index.Load("test4"); !exist {
		t.Error("failed to insert")
	}
	if v, exist := db.index.Load("test3"); !exist || v.(Record).last.value != "new_value3" {
		t.Error("failed to update")
	}
}

func generateTestData() {
	walFile, err := os.Create(TestWALFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer walFile.Close()
	dbFile, err := os.Create(TestDBFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer dbFile.Close()

	// test data -> db-file
	for i := 1; i < 4; i++ {
		line := fmt.Sprintf("test%v value%v\n", i, i)
		_, err = dbFile.WriteString(line)
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := dbFile.Sync(); err != nil {
		log.Println("cannot sync db-file")
	}

	// test data -> write-set
	testWriteSet := make(WriteSet)
	testWriteSet["test4"] = append(testWriteSet["test4"], &Operation{INSERT, &Version{"test4", "value4", 0, 0, nil, false}})
	testWriteSet["test3"] = append(testWriteSet["test3"], &Operation{UPDATE, &Version{"test3", "new_value3", 0, 0, nil, false}})
	testWriteSet["test2"] = append(testWriteSet["test2"], &Operation{DELETE, &Version{"test2", "", 0, 0, nil, true}})

	// write-set -> wal-file
	buf := make([]byte, 4096)
	idx := uint(0)
	for _, operations := range testWriteSet {
		for _, op := range operations {
			checksum := crc32.ChecksumIEEE([]byte(op.version.key))

			// serialize data
			size := serialize(buf, idx, op, checksum)
			idx += size
		}
	}
	if _, err := walFile.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := walFile.Sync(); err != nil {
		log.Println("cannot sync wal-file")
	}
}

func NewTestDB() *DB {
	walFile, err := os.OpenFile(TestWALFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	dbFile, err := os.OpenFile(TestDBFileName, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	return &DB{
		wALFile: walFile,
		dBFile:  dbFile,
		index:   sync.Map{},
	}
}
