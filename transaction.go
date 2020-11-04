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
	ID 	uint
	WalFile 	*os.File
	WriteSet 	WriteSet
	Index 		Index // 共有
}

func NewTx(id uint, walFile *os.File, index Index) *Tx {
	return &Tx{
		ID:      	id,
		WalFile: 	walFile,
		WriteSet: 	WriteSet{},
		Index: 		index,
	}
}

func (tx *Tx) DestructTx() {
	tx.WriteSet = WriteSet{}
	if err := tx.WalFile.Close(); err != nil {
		log.Println(err)
	}
}

func (tx *Tx) Read(key string) error {
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist == "" {
		return errors.New("key not exists")
	} else {
		fmt.Println(exist)
	}
	return nil
}

func (tx *Tx) Insert(key, value string) error {
	record := Record{key, value}
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist != "" {
		return errors.New("key already exists")
	}
	tx.WriteSet = append(tx.WriteSet, Operation{INSERT, record})
	return nil
}

func (tx *Tx) Update(key, value string) error {
	record := Record{key, value}
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist == "" {
		return errors.New("key not exists")
	}
	tx.WriteSet = append(tx.WriteSet, Operation{UPDATE, record})
	return nil
}

func (tx *Tx) Delete(key string) error {
	record := Record{Key: key}
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist == "" {
		return errors.New("key not exists")
	}
	tx.WriteSet = append(tx.WriteSet, Operation{DELETE, record})
	return nil
}

func (tx *Tx) Commit() {
	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	for i := 0; i < len(tx.WriteSet); i++ {
		op := tx.WriteSet[i]
		switch op.CMD {
		case INSERT:
			tx.Index[op.Key] = op.Value
		case UPDATE:
			tx.Index[op.Key] = op.Value
		case DELETE:
			delete(tx.Index, op.Key)
		}
	}
	// delete write-set
	tx.WriteSet = WriteSet{}
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

	for i := 0; i < len(tx.WriteSet); i++ {
		op := tx.WriteSet[i]
		checksum := crc32.ChecksumIEEE([]byte(op.Key))

		// serialize data
		size := serialize(buf, idx, op, checksum)
		idx += size
	}
	if _, err := tx.WalFile.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := tx.WalFile.Sync(); err != nil {
		log.Println("cannot sync wal-file")
	}
}