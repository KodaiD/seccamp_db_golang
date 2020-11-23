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
	key   string
	first *Version
	last  *Version
	mu    *sync.Mutex
}

type Version struct {
	key     string
	value   string
	wTs     uint
	rTs     uint // こいつだけatomicにやるとか
	prev    *Version
	deleted bool
}

type Operation struct {
	cmd     uint
	version *Version
}

type WriteSet map[string][]*Operation
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
	v, where := tx.getVersion(key)
	if where == InWriteSet || where == InReadSet {
		return v.value, nil
	} else {
		record := tx.getRecord(key)
		if record == nil {
			v := Version{
				key:     key,
				value:   "",
				wTs:     tx.ts,
				rTs:     tx.ts,
				prev:    nil,
				deleted: true,
			}
			tx.readSet[key] = &v
			return "", errors.New("key doesn't exist")
		}

		record.mu.Lock()
		defer record.mu.Unlock()

		cur := record.last
		for cur.rTs > tx.ts {
			cur = cur.prev
		}
		cur.rTs = tx.ts
		tx.readSet[key] = cur
		return cur.value, nil
	}
}

func (tx *Tx) Insert(key, value string) error {
	_, where := tx.getVersion(key)
	if where == NotExist || where == Deleted {
		v := Version{
			key:     key,
			value:   value,
			wTs:     tx.ts,
			rTs:     tx.ts,
			prev:    nil,
			deleted: false,
		}
		tx.writeSet[key] = append(tx.writeSet[key], &Operation{cmd: INSERT, version: &v})
		return nil
	}
	return errors.New("key already exists")
}

func (tx *Tx) Update(key, value string) error {
	_, where := tx.getVersion(key)
	if where == NotExist || where == Deleted {
		return errors.New("key doesn't exist")
	}
	v := Version{
		key:     key,
		value:   value,
		wTs:     tx.ts,
		rTs:     tx.ts,
		prev:    nil,
		deleted: false,
	}
	tx.writeSet[key] = append(tx.writeSet[key], &Operation{cmd: UPDATE, version: &v})
	return nil
}

func (tx *Tx) Delete(key string) error {
	_, where := tx.getVersion(key)
	if where == NotExist || where == Deleted {
		return errors.New("key doesn't exist")
	}
	v := Version{
		key:     key,
		value:   "",
		wTs:     tx.ts,
		rTs:     tx.ts,
		prev:    nil,
		deleted: true,
	}
	tx.writeSet[key] = append(tx.writeSet[key], &Operation{cmd: DELETE, version: &v})
	return nil
}

func (tx *Tx) Commit() error {
	// serialization point

	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	var history []*Version
	for _, operations := range tx.writeSet {
		for _, op := range operations {
			switch op.cmd {
			case INSERT:
				if v, exist := tx.db.index.Load(op.version.key); exist || !v.(Record).last.deleted {
					rollback(history)
					return errors.New("failed to commit")
				}
				record := Record{
					key:   op.version.key,
					first: op.version,
					last:  op.version,
					mu:    new(sync.Mutex),
				}
				tx.db.index.Store(op.version.key, record)
				history = append(history, op.version)
			case UPDATE:
				v, exist := tx.db.index.Load(op.version.key)
				if !exist {
					rollback(history)
					return errors.New("failed to commit")
				}
				record := v.(Record)
				record.mu.Lock()
				latest := record.last
				if tx.ts < latest.rTs {
					rollback(history)
					break
				} else if tx.ts >= latest.rTs {
					op.version.prev = latest
					record.last = op.version
				}
				record.mu.Unlock()
				history = append(history, op.version)
			case DELETE:
				v, exist := tx.db.index.Load(op.version.key)
				if !exist {
					rollback(history)
					return errors.New("failed to commit")
				}
				record := v.(Record)
				record.mu.Lock()
				latest := record.last
				if tx.ts < latest.rTs {
					rollback(history)
					tx.writeSet = make(WriteSet)
					tx.readSet = make(ReadSet)
					return errors.New("failed to commit")
				} else if tx.ts >= latest.rTs {
					op.version.prev = latest
					record.last = op.version
				}
				record.mu.Unlock()
				history = append(history, op.version)
			}
		}
	}

	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
	return nil
}

func (tx *Tx) Abort() {
	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
	fmt.Println("Abort!")
}

func (tx *Tx) getRecord(key string) *Record {
	if v, exist := tx.db.index.Load(key); exist {
		record := v.(Record)
		if record.last.deleted {
			return nil
		}
		return &record
	}
	return nil
}

func (tx *Tx) getVersion(key string) (*Version, uint) {
	// check write-set
	for k, operations := range tx.writeSet {
		for _, op := range operations {
			if key == k {
				if op.version.deleted {
					return nil, Deleted
				}
				return op.version, InWriteSet
			}
		}
	}

	// check read-set
	if version, exist := tx.readSet[key]; exist {
		if version.deleted {
			return nil, Deleted
		}
		return version, InReadSet
	}

	// check index
	if v, exist := tx.db.index.Load(key); exist {
		record := v.(Record)
		if record.last.deleted {
			return nil, Deleted
		}
		return record.last, InIndex
	}
	return nil, NotExist
}

func (tx *Tx) SaveWal() {
	// make redo log
	buf := make([]byte, 4096)
	idx := uint(0) // 書き込み開始位置

	for _, operations := range tx.writeSet {
		for _, op := range operations {
			checksum := crc32.ChecksumIEEE([]byte(op.version.key))

			// serialize data
			size := serialize(buf, idx, op, checksum)
			idx += size
		}
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

func rollback(s []*Version) {
	for i := len(s) - 1; 0 <= i; i-- {
		s[i].prev = nil
	}
}

// read all data in db-memory
func readAll(index *sync.Map) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	index.Range(func(k, v interface{}) bool {
		key := k.(string)
		record := v.(Record)
		fmt.Printf("%s		| %s\n", key, record.last.value)
		return true
	})
	fmt.Println("----------------------------")
}
