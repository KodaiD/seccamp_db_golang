package main

import (
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"sync"
	"testing"
	"time"
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
	if record, _ := db.index.data["test1"]; record.last.value != "value1" {
		t.Error("failed to load data")
	}
}

func TestDB_LoadWal(t *testing.T) {
	db := NewTestDB()
	generateTestData()
	defer db.dBFile.Close()
	defer db.wALFile.Close()

	v1 := &Version{
		key:   "key1",
		value: "value1",
		wTs:   0,
		rTs:   0,
		next:  nil,
		mu:    nil,
	}
	v2 := &Version{
		key:   "key2",
		value: "value2",
		wTs:   0,
		rTs:   0,
		next:  nil,
		mu:    nil,
	}
	v3 := &Version{
		key:   "key3",
		value: "value3",
		wTs:   0,
		rTs:   0,
		next:  nil,
		mu:    nil,
	}

	db.index.data["key1"] = Record{
		key:   "key1",
		first: v1,
		last:  v1,
	}
	db.index.data["key2"] = Record{
		key:   "key2",
		first: v2,
		last:  v2,
	}
	db.index.data["key3"] = Record{
		key:   "key3",
		first: v3,
		last:  v3,
	}

	// crash recovery (wal-file -> db-memory)
	db.loadWal()
	if _, exist := db.index.data["test4"]; exist != true {
		t.Error("failed to insert")
	}
	if record, exist := db.index.data["test3"]; exist == false || record.last.value != "new_value3" {
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
	testWriteSet["test4"] = &Operation{INSERT, &Version{"test4", "value4", 0, 0, nil, new(sync.Mutex)}, time.Now()}
	testWriteSet["test3"] = &Operation{UPDATE, &Version{"test3", "new_value3", 0, 0, nil, new(sync.Mutex)}, time.Now()}
	testWriteSet["test2"] = &Operation{DELETE, &Version{"test2", "", 0, 0, nil, new(sync.Mutex)}, time.Now()}

	// write-set -> wal-file
	buf := make([]byte, 4096)
	idx := uint(0)
	for _, op := range testWriteSet {
		checksum := crc32.ChecksumIEEE([]byte(op.version.key))

		// serialize data
		size := serialize(buf, idx, op, checksum)
		idx += size
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
		index:   Index{make(map[string]Record), new(sync.RWMutex)},
	}
}