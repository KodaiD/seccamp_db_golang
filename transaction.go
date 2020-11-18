package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
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
	first *Version
	last  *Version
}

type Version struct {
	key   string
	value string
	wTs   uint
	rTs   uint
	next  *Version
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
	v, where := tx.checkExistence(key)
	if where == InReadSet || where == InWriteSet || where == InIndex {
		v.mu.Lock()
		if v.rTs < tx.ts {
			v.rTs = tx.ts
		}
		tx.readSet[v.key] = v
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
		return nil
	} else {
		return errors.New("key already exists")
	}
}

func (tx *Tx) Update(key, value string) error {
	v, where := tx.checkExistence(key)
	if where == InReadSet || where == InWriteSet || where == InIndex {
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
			v.next = newV
		}
		tx.writeSet[key] = &Operation{UPDATE, v}
		v.mu.Unlock()
		return nil
	} else {
		return errors.New("key doesn't exist")
	}
}

func (tx *Tx) Delete(key string) error {
	v, where := tx.checkExistence(key)
	if where == InReadSet || where == InWriteSet || where == InIndex {
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
			v.next = newV
		}
		tx.writeSet[key] = &Operation{DELETE, v}
		v.mu.Unlock()
		return nil
	} else {
		return errors.New("key doesn't exist")
	}
}

func (tx *Tx) Commit() {
	// serialization point

	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	tx.db.index.mu.Lock()
	for _, op := range tx.writeSet {
		switch op.cmd {
		case INSERT:
			record := Record{
				key:   op.version.key,
				first: op.version,
				last:  op.version,
			}
			tx.db.index.data[op.version.key] = record
		case UPDATE:
			record := tx.db.index.data[op.version.key]
			record.last = op.version
		case DELETE:
			delete(tx.db.index.data, op.version.key)
		}
	}
	tx.db.index.mu.Unlock()

	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
}

func (tx *Tx) Abort() {
	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
	fmt.Println("Abort!")
}

func (tx *Tx) checkExistence(key string) (*Version, uint) {
	// check write-set
	for k, op := range tx.writeSet {
		if op.cmd != DELETE && key == k {
			return op.version, InWriteSet
		}
	}

	// check read-set
	if version, exist := tx.readSet[key]; exist {
		return version, InReadSet
	}

	// check index
	tx.db.index.mu.Lock()
	if record, exist := tx.db.index.data[key]; exist {
		return record.last, InIndex
	}
	tx.db.index.mu.Unlock()

	return nil, NotExist
}

func (tx *Tx) SaveWal() {
	// make redo log
	buf := make([]byte, 4096)
	idx := uint(0) // 書き込み開始位置

	for _, op := range tx.writeSet {
		checksum := crc32.ChecksumIEEE([]byte(op.version.key))

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
func readAll(index *Index) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	for key, record := range index.data {
		fmt.Printf("%s		| %s\n", key, record.last.value)
	}
	fmt.Println("----------------------------")
}