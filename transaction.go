package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	InReadSet = 1 + iota
	InWriteSet
	InIndex
	Deleted
	NotInRWSet
	NotExist
)

type Record struct {
	key   string
	first *Version
	last  *Version
	mu    sync.Mutex
}

type Version struct { // TODO: gc
	key     string
	value   string
	wTs     uint64
	rTs     uint64
	prev    *Version
	deleted bool
}

type Operation struct {
	cmd     uint8
	version *Version
}

type WriteSet map[string][]*Operation
type ReadSet map[string]*Version

type Tx struct {
	ts       uint64
	writeSet WriteSet
	readSet  ReadSet
	db       *DB
}

func NewTx(db *DB) *Tx {
	return &Tx{
		ts:       atomic.AddUint64(&db.tsGenerator, 1),
		writeSet: make(WriteSet),
		readSet:  make(ReadSet),
		db:       db,
	}
}

func (tx *Tx) DestructTx() {
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
}

func (tx *Tx) Read(key string) (string, error) {
	// data in read/write-set
	version, where := tx.checkExistence(key)
	if where == InWriteSet || where == InReadSet {
		return version.value, nil
	}

	version = &Version{
		key:     key,
		value:   "",
		wTs:     tx.ts,
		rTs:     tx.ts,
		prev:    nil,
		deleted: true, // prevent phantom problem
	}
	record := &Record{
		key:   key,
		first: version,
		last:  version,
		mu:    sync.Mutex{},
	}
	v, exist := tx.db.index.LoadOrStore(key, &record)
	// data does not exist
	if !exist {
		tx.readSet[key] = version
		return "", errors.New("key doesn't exist")
	}

	// data in index
	record = v.(*Record)
	record.mu.Lock()
	defer record.mu.Unlock()
	cur := record.last
	for cur.rTs > tx.ts {
		if cur.deleted { // delete flag check
			return "", errors.New("key doesn't exist")
		}
		cur = cur.prev
		if cur == nil {
			break
		}
	}
	if cur == nil { // cannot traverse
		return "", errors.New("key doesn't exist")
	}
	cur.rTs = tx.ts
	tx.readSet[key] = cur
	return cur.value, nil
}

func (tx *Tx) Insert(key, value string) error {
	_, where := tx.checkExistence(key) // read/write-set の確認だけにする、ここでdeletedの確認をしたところで、commit時には変わっているかもしれない
	if where == NotInRWSet {
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
	_, where := tx.checkExistence(key) // read/write-set の確認だけにする
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
	_, where := tx.checkExistence(key) // read/write-set の確認だけにする
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

	var err error

	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	// 一括ロック
	var sortedWriteSet []*Operation
	for _, ops := range tx.writeSet {
		for _, op := range ops {
			sortedWriteSet = append(sortedWriteSet, op)
		}
	}
	sort.SliceStable(sortedWriteSet, func(i, j int) bool { // prevent deadlock
		return sortedWriteSet[i].version.key < sortedWriteSet[j].version.key
	})
	lockedRecord := make(map[string]*Record, len(sortedWriteSet))
	for _, op := range sortedWriteSet {
		var record *Record
		v, exist := tx.db.index.Load(op.version.key)
		if !exist {
			if op.cmd == UPDATE || op.cmd == DELETE {
				err = errors.New("failed to commit")
				goto unlock
			}
			record = &Record{
				key:   op.version.key,
				first: op.version,
				last:  op.version,
				mu:    sync.Mutex{},
			}
			record.mu.Lock()
			tx.db.index.Store(op.version.key, &record)
		} else {
			if op.cmd == INSERT {
				err = errors.New("failed to commit")
				goto unlock
			}
			record = v.(*Record)
			record.mu.Lock()
		}
		lockedRecord[op.version.key] = record
	}

	for _, op := range sortedWriteSet {
		switch op.cmd {
		case INSERT:
			record := lockedRecord[op.version.key]
			if !record.last.deleted {
				err = errors.New("failed to commit INSERT")
				break
			}
		case UPDATE:
			record := lockedRecord[op.version.key]
			latest := record.last
			if tx.ts < latest.rTs {
				err = errors.New("failed to commit UPDATE")
				break
			} else if tx.ts >= latest.rTs {
				op.version.prev = latest
				record.last = op.version
			}
		case DELETE:
			record := lockedRecord[op.version.key]
			latest := record.last
			if tx.ts < latest.rTs {
				err = errors.New("failed to commit DELETE")
				break
			} else if tx.ts >= latest.rTs {
				op.version.prev = latest
				record.last = op.version
			}
		}
	}

	// 一括アンロック
unlock:
	for _, record := range lockedRecord {
		record.mu.Unlock()
	}

	return err
}

func (tx *Tx) Abort() {
	// delete read/write-set
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
	fmt.Println("Abort!")
}

func (tx *Tx) checkExistence(key string) (*Version, uint) {
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
	return nil, NotInRWSet
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

// read all data in db-memory
func readAll(index *sync.Map) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	index.Range(func(k, v interface{}) bool {
		key := k.(string)
		record := v.(*Record)
		fmt.Printf("%s		| %s\n", key, record.last.value)
		return true
	})
	fmt.Println("----------------------------")
}
