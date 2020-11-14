package main

import (
	"errors"
	"fmt"
	"github.com/KodaiD/rwumutex"
	"hash/crc32"
	"log"
	"sync"
)

const (
	InReadSet = 1 + iota
	InWriteSet
	InIndex
	NotExist
)

type Record struct {
	key     string
	value   string
	mu      *rwuMutex.RWUMutex
	deleted bool // db-memory
}

type Operation struct {
	cmd    uint
	record *Record
}

type WriteSet map[string]*Operation
type ReadSet map[string]*Record

type Tx struct {
	id       uint
	writeSet WriteSet
	readSet  ReadSet
	db       *DB
}

func NewTx(id uint, db *DB) *Tx {
	return &Tx{
		id:       id,
		writeSet: make(WriteSet),
		readSet:  make(ReadSet),
		db:       db,
	}
}

func (tx *Tx) DestructTx() {
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
	if err := tx.db.wALFile.Close(); err != nil {
		log.Println(err)
	}
}

func (tx *Tx) Read(key string) error {
	record, where := tx.checkExistence(key)
	switch where {
	case InReadSet:
		fmt.Println(record.value)
	case InWriteSet:
		fmt.Println(record.value)
	case InIndex:
		if !record.mu.TryRLock() {
			tx.Abort()
			return errors.New("abort")
		}
		tx.readSet[record.key] = record
	case NotExist: // prevent phantom read
		record = &Record{key, "", new(rwuMutex.RWUMutex), true}
		if !record.mu.TryRLock() {
			tx.Abort()
			return errors.New("abort")
		}
		tx.db.index.Store(record.key, record)
		return errors.New("key doesn't exist")
	}
	return nil
}

func (tx *Tx) Insert(key, value string) error {
	record := Record{key, value, new(rwuMutex.RWUMutex), false}
	if _, where := tx.checkExistence(key); where != NotExist {
		return errors.New("key already exists")
	}
	if !record.mu.TryLock() {
		tx.Abort()
		return errors.New("abort")
	}
	tx.writeSet[key] = &Operation{INSERT, &record}
	return nil
}

func (tx *Tx) Update(key, value string) error {
	record, where := tx.checkExistence(key)
	switch where {
	case InReadSet:
		if !record.mu.TryUpgrade() {
			tx.Abort()
			return errors.New("abort")
		}
	case InWriteSet:
		//
	case InIndex:
		if !record.mu.TryLock() {
			tx.Abort()
			return errors.New("abort")
		}
	case NotExist:
		return errors.New("key doesn't exist")
	}
	record.value = value
	tx.writeSet[key] = &Operation{UPDATE, record}
	return nil
}

func (tx *Tx) Delete(key string) error {
	record, where := tx.checkExistence(key)
	switch where {
	case InReadSet:
		if !record.mu.TryUpgrade() {
			tx.Abort()
			return errors.New("abort")
		}
	case InWriteSet:
		//
	case InIndex:
		if !record.mu.TryLock() {
			tx.Abort()
			return errors.New("abort")
		}
	case NotExist:
		return errors.New("key doesn't exist")
	}
	record.value = ""
	tx.writeSet[key] = &Operation{DELETE, record}
	return nil
}

func (tx *Tx) Commit() {
	// serialization point

	// unlock all read lock
	for _, record := range tx.readSet {
		record.mu.RUnlock()
	}

	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	for _, op := range tx.writeSet {
		switch op.cmd {
		case INSERT:
			tx.db.index.Store(op.record.key, *op.record)
			op.record.mu.Unlock()
		case UPDATE:
			tx.db.index.Store(op.record.key, *op.record)
			op.record.mu.Unlock()
		case DELETE:
			// delete(tx.db.index, op.record.key) これはまずい
			// 論理 delete
			op.record.deleted = true
			tx.db.index.Store(op.record.key, *op.record)
			op.record.mu.Unlock()
		}
	}

	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
}

func (tx *Tx) Abort() {
	// unlock
	for _, record := range tx.readSet {
		record.mu.RUnlock()
	}
	for _, op := range tx.writeSet {
		op.record.mu.Unlock()
	}

	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)

	fmt.Println("Abort!")
}

func (tx *Tx) checkExistence(key string) (*Record, uint) {
	// check write-set
	for _, op := range tx.writeSet {
		if op.cmd != DELETE && key == op.record.key {
			return op.record, InWriteSet
		}
	}
	// check read-set
	if record, exist := tx.readSet[key]; exist {
		return record, InReadSet
	}
	// check index
	if v, exist := tx.db.index.Load(key); exist {
		record := v.(Record)
		if !record.deleted {
			return &record, InIndex
		}
	}
	return nil, NotExist
}

func (tx *Tx) SaveWal() {
	// make redo log
	buf := make([]byte, 4096)
	idx := uint(0) // 書き込み開始位置

	for _, op := range tx.writeSet {
		checksum := crc32.ChecksumIEEE([]byte(op.record.key))

		// serialize data
		size := serialize(buf, idx, op, checksum)
		idx += size
	}

	tx.db.walMu.Lock()
	if _, err := tx.db.wALFile.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := tx.db.wALFile.Sync(); err != nil {
		log.Println("cannot sync wal-file")
	}
	tx.db.walMu.Unlock()
}

// read all data in db-memory
func readAll(index *sync.Map) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	index.Range(func(k, v interface{}) bool {
		key := k.(string)
		record := v.(Record)
		fmt.Printf("%s		| %s\n", key, record.value)
		return true
	})
	fmt.Println("----------------------------")
}