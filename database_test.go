package main

import (
	"fmt"
	rwuMutex "github.com/KodaiD/rwumutex"
	"hash/crc32"
	"log"
	"os"
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
	if len(db.index) != 3 || db.index["test1"].value != "value1" {
		t.Error("failed to load data")
	}
}

func TestDB_LoadWal(t *testing.T) {
	db := NewTestDB()
	generateTestData()
	defer db.dBFile.Close()
	defer db.wALFile.Close()

	// crash recovery (wal-file -> db-memory)
	db.loadWal()
	if len(db.index) != 2 {
		t.Error("failed to load wal (insert or delete log)")
	}
	if _, exist := db.index["test4"]; exist != true {
		t.Error("failed to insert")
	}
	if record, exist := db.index["test3"]; exist == false || record.value != "new_value3" {
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

	// test data -> wal-file
	testWriteSet := make(WriteSet)
	testWriteSet["test4"] = &Operation{INSERT, &Record{"test4", "value4", new(rwuMutex.RWUMutex), false}}
	testWriteSet["test3"] = &Operation{UPDATE, &Record{"test3", "new_value3", new(rwuMutex.RWUMutex), false}}
	testWriteSet["test2"] = &Operation{DELETE, &Record{"test2", "", new(rwuMutex.RWUMutex), false}}

	buf := make([]byte, 4096)
	idx := uint(0)
	for _, op := range testWriteSet {
		checksum := crc32.ChecksumIEEE([]byte(op.record.key))

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
		index:   make(Index),
	}
}