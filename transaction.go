package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
)

type Record struct {
	Key 	string
	Value 	string
}

type Operation struct {
	CMD 	uint
	Record
}

type WriteSet []Operation

type Tx struct {
	id       uint
	writeSet WriteSet
	db       *DB
}

func NewTx(id uint, db *DB) *Tx {
	return &Tx{
		id:       id,
		writeSet: WriteSet{},
		db: db,
	}
}

func (tx *Tx) DestructTx() {
	tx.writeSet = WriteSet{}
	if err := tx.db.WALFile.Close(); err != nil {
		log.Println(err)
	}
}

func (tx *Tx) Read(key string) error {
	exist := checkExistence(tx.db.Index, tx.writeSet, key)
	if exist == "" {
		return errors.New("key not exists")
	} else {
		fmt.Println(exist)
	}
	return nil
}

func (tx *Tx) Insert(key, value string) error {
	record := Record{key, value}
	exist := checkExistence(tx.db.Index, tx.writeSet, key)
	if exist != "" {
		return errors.New("key already exists")
	}
	tx.writeSet = append(tx.writeSet, Operation{INSERT, record})
	return nil
}

func (tx *Tx) Update(key, value string) error {
	record := Record{key, value}
	exist := checkExistence(tx.db.Index, tx.writeSet, key)
	if exist == "" {
		return errors.New("key not exists")
	}
	tx.writeSet = append(tx.writeSet, Operation{UPDATE, record})
	return nil
}

func (tx *Tx) Delete(key string) error {
	record := Record{Key: key}
	exist := checkExistence(tx.db.Index, tx.writeSet, key)
	if exist == "" {
		return errors.New("key not exists")
	}
	tx.writeSet = append(tx.writeSet, Operation{DELETE, record})
	return nil
}

func (tx *Tx) Commit() {
	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	for i := 0; i < len(tx.writeSet); i++ {
		op := tx.writeSet[i]
		switch op.CMD {
		case INSERT:
			tx.db.Index[op.Key] = op.Value
		case UPDATE:
			tx.db.Index[op.Key] = op.Value
		case DELETE:
			delete(tx.db.Index, op.Key)
		}
	}
	// delete write-set
	tx.writeSet = WriteSet{}
}

func (tx *Tx) Abort() {
	os.Exit(1)
}

// write-set から指定された key の record の存在を調べる
func checkExistence(index Index, writeSet WriteSet, key string) string {
	// check write-set
	for i := len(writeSet) - 1; 0 <= i; i-- {
		operation := writeSet[i]
		if key == operation.Record.Key {
			if operation.CMD == DELETE {
				return ""
			}
			return operation.Record.Value
		}
	}
	// check Index
	value, exist := index[key]
	if !exist {
		return ""
	}
	return value
}

// read all data in db-memory
func readAll(index Index) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	for k, v := range index {
		fmt.Printf("%s		| %s\n", k, v)
	}
	fmt.Println("----------------------------")
}

func (tx *Tx) SaveWal()  {
	// make redo log
	buf := make([]byte, 4096)
	idx := uint(0) // 書き込み開始位置

	for i := 0; i < len(tx.writeSet); i++ {
		op := tx.writeSet[i]
		checksum := crc32.ChecksumIEEE([]byte(op.Key))

		// serialize data
		size := serialize(buf, idx, op, checksum)
		idx += size
	}

	tx.db.walMu.Lock()
	if _, err := tx.db.WALFile.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := tx.db.WALFile.Sync(); err != nil {
		log.Println("cannot sync wal-file")
	}
	tx.db.walMu.Unlock()
}