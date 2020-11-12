package main

// TODO: 

//import (
//	"bufio"
//	"fmt"
//	"hash/crc32"
//	"log"
//	"os"
//	"testing"
//)
//
const (
	TestDBFileName  = "seccampdb.db"
	TestWALFileName = "seccampdb.log"
)
//
//func TestDB_LoadData(t *testing.T) {
//	db := NewDB(TestWALFileName, TestDBFileName)
//	generateTestData(db.wALFile, db.dBFile)
//
//	// crash recovery (db-file -> db-memory)
//	db.loadData()
//	if len(db.index) != 3 || db.index["test1"] != "value1" {
//		t.Error("failed to load data")
//	}
//}
//
//func TestDB_LoadWal(t *testing.T) {
//	db := NewDB(TestWALFileName, TestDBFileName)
//	generateTestData(db. wALFile, db.dBFile)
//
//	// crash recovery (wal-file -> db-memory)
//	db.loadWal()
//	if len(db.index) != 3 {
//		t.Error("failed to load wal (insert or delete log)")
//	}
//	if _, exist := db.index["test4"]; exist != true {
//		t.Error("failed to insert")
//	}
//	if value, exist := db.index["test3"]; exist == false || value != "new_value3" {
//		t.Error("failed to update")
//	}
//}
//
//func TestDB_SaveData(t *testing.T) {
//	db := NewDB(TestWALFileName, TestDBFileName)
//	db.index = map[string]string{
//		"key1": "value1",
//		"key2": "value2",
//		"key3": "value3",
//	}
//
//	// checkpointing (db-memory -> db-file)
//	db.saveData()
//	scanner := bufio.NewScanner(db.dBFile)
//	for i := 1; i < 4; i++ {
//		line := scanner.Text()
//		ans := fmt.Sprintf("key%v value%v", i, i)
//		fmt.Println(line)
//		fmt.Println(ans)
//		if line != ans {
//			t.Error("failed to save data")
//		}
//	}
//}
//
//func generateTestData(walFile, dbFile *os.File) {
//	// test data -> db-file
//	for i := 1; i < 4; i++ {
//		line := fmt.Sprintf("test%v value%v\n", i, i)
//		_, err := dbFile.WriteString(line)
//		if err != nil {
//			log.Fatal(err)
//		}
//	}
//	if err := dbFile.Sync(); err != nil {
//		log.Println("cannot sync db-file")
//	}
//
//	// test data -> wal-file
//	testWriteSet := writeSet{
//		Operation{INSERT, Record{"test4", "value4"}},
//		Operation{UPDATE, Record{"test3", "new_value3"}},
//		Operation{DELETE, Record{"test2", ""}},
//	}
//	buf := make([]byte, 4096)
//	idx := uint(0)
//	for i := 0; i < len(testWriteSet); i++ {
//		op := testWriteSet[i]
//		checksum := crc32.ChecksumIEEE([]byte(op.key))
//
//		// serialize data
//		size := serialize(buf, idx, op, checksum)
//		idx += size
//	}
//	if _, err := walFile.Write(buf); err != nil {
//		log.Fatal(err)
//	}
//	if err := walFile.Sync(); err != nil {
//		log.Println("cannot sync wal-file")
//	}
//}