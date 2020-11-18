package main

import (
	"errors"
	"fmt"
	"github.com/KodaiD/rwumutex"
	"hash/crc32"
	"log"
	"sync"
	"time"
)

const (
	InReadSet = 1 + iota
	InWriteSet
	InIndex
	Deleted
	NotExist
)

type Record struct {
	key 	string
	version []*Version
}

type Version struct {
	key   string
	value string
	wTs   uint
	rTs   uint
	mu    sync.Mutex
}

type Operation struct {
	cmd    uint
	version *Version
}

type WriteSet map[string]*Operation
type ReadSet map[string]*Version

type Tx struct {
	ts       uint
	writeSet WriteSet
	readSet  ReadSet
	db       *DB
}

func NewTx(ts uint, db *DB) *Tx {
	return &Tx{
		ts:       ts,
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

func (tx *Tx) Read(key string) (string, error) {
	record, where := tx.checkExistence(key)
	if where == InReadSet || where == InWriteSet || where == InIndex {
		v := record.version[len(record.version)-1]
		v.mu.Lock()
		if record.version[len(record.version)-1].rTs < tx.ts {
			record.version[len(record.version)-1].rTs = tx.ts
		}
		tx.readSet[record.key] = v
		v.mu.Unlock()
		return v.value, nil
	} else {
		return "", errors.New("key doesn't exist")
	}
}

func (tx *Tx) Insert(key, value string) error {
	_, where := tx.checkExistence(key)
	if where == NotExist || where == Deleted {
		v := Version {
			key:   key,
			value: value,
			wTs:   tx.ts,
			rTs:   tx.ts,
			mu:    sync.Mutex{},
		}
		tx.writeSet[key] = &Operation{cmd: INSERT, version: &v}
	} else {
		return errors.New("key already exists")
	}
}

func (tx *Tx) Update(key, value string) error {
	record, where := tx.checkExistence(key)
	if where == InReadSet || where == InWriteSet || where == InIndex {
		v := record.version[len(record.version)-1]
		v.mu.Lock()
		if tx.ts < v.rTs {
			// rollback()
		} else if tx.ts == v.wTs {
			v.value = value
		} else if tx.ts > v.rTs {
			newV := &Version{
				key:   key,
				value: value,
				wTs:   tx.ts,
				rTs:   tx.ts,
			}
			record.version = append(record.version, newV)
			v = newV
		}
		tx.writeSet[key] = &Operation{UPDATE, v}
		v.mu.Unlock()
		return nil
	} else {
		return errors.New("key doesn't exist")
	}
}

func (tx *Tx) Delete(key string) error {
	record, where := tx.checkExistence(key)
	if where == InReadSet || where == InWriteSet || where == InIndex {
		v := record.version[len(record.version)-1]
		v.mu.Lock()
		if tx.ts < v.rTs {
			// rollback()
		} else if tx.ts == v.wTs {
			v.value = ""
		} else if tx.ts > v.rTs {
			newV := &Version{
				key:   key,
				value: "",
				wTs:   tx.ts,
				rTs:   tx.ts,
			}
			record.version = append(record.version, newV)
			v = newV
		}
		tx.writeSet[key] = &Operation{DELETE, v}
		v.mu.Unlock()
		return nil
	} else {
		return errors.New("key doesn't exist")
	}
}

// =====================================================

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
		if record.deleted {
			return &record, Deleted
		}
		return &record, InIndex
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