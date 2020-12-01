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
	last  *Version
	mu    sync.Mutex
}

type Version struct {
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
	ts := atomic.AddUint64(&db.tsGenerator, 1)
	tx :=  &Tx{
		ts:       ts,
		writeSet: make(WriteSet),
		readSet:  make(ReadSet),
		db:       db,
	}
	tx.db.aliveTx.mu.Lock()
	defer tx.db.aliveTx.mu.Unlock()
	db.aliveTx.txs = append(db.aliveTx.txs, ts)
	return tx
}

func (tx *Tx) DestructTx() {
	tx.writeSet = make(WriteSet)
	tx.readSet = make(ReadSet)
	tx.db.aliveTx.mu.Lock()
	defer tx.db.aliveTx.mu.Unlock()
	idx := 0
	for i, ts := range tx.db.aliveTx.txs {
		if tx.ts == ts {
			idx = i
			break
		}
	}
	tx.db.aliveTx.txs = append(tx.db.aliveTx.txs[:idx], tx.db.aliveTx.txs[idx+1:]...)
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
		last:  version,
		mu:    sync.Mutex{},
	}
	v, exist := tx.db.index.LoadOrStore(key, record)
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
	for cur.wTs > tx.ts {
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
	if cur.deleted { // delete flag check
		return "", errors.New("key doesn't exist")
	}
	cur.rTs = tx.ts
	tx.readSet[key] = cur
	return cur.value, nil
}

func (tx *Tx) Insert(key, value string) error {
	_, where := tx.checkExistence(key) // read/write-set の確認だけにする、ここでdeletedの確認をしたところで、commit時には変わっているかもしれない
	if where == NotInRWSet || where == Deleted {
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
	if where == Deleted {
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
	if where == Deleted {
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
	var err error

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

	// 一括ロック
	for _, op := range sortedWriteSet {
		switch op.cmd {
		case INSERT:
			v, exist := tx.db.index.Load(op.version.key)
			if exist {
				record := v.(*Record)
				record.mu.Lock()
				lockedRecord[op.version.key] = record
				if !record.last.deleted {
					err = errors.New("failed to commit INSERT")
					goto unlock
				}
				continue
			}
			op.version.deleted = true
			record := &Record{
				key:   op.version.key,
				last:  op.version,
				mu:    sync.Mutex{},
			}
			record.mu.Lock()
			lockedRecord[op.version.key] = record
			_, exist = tx.db.index.LoadOrStore(op.version.key, record)
			if exist {
				err = errors.New("failed to commit INSERT")
				goto unlock
			}
		case UPDATE:
			_, exist := lockedRecord[op.version.key]
			if exist {
				continue
			}

			v, exist := tx.db.index.Load(op.version.key)
			if !exist {
				err = errors.New("failed to commit UPDATE")
				goto unlock
			}
			record := v.(*Record)
			record.mu.Lock()
			lockedRecord[op.version.key] = record
			if record.last.deleted {
				err = errors.New("failed to commit UPDATE")
				goto unlock
			}
			if tx.ts < record.last.rTs {
				err = errors.New("failed to commit UPDATE")
				goto unlock
			}
		case DELETE:
			_, exist := lockedRecord[op.version.key]
			if exist {
				continue
			}
			v, exist := tx.db.index.Load(op.version.key)
			if !exist {
				err = errors.New("failed to commit DELETE")
				goto unlock
			}
			record := v.(*Record)
			record.mu.Lock()
			lockedRecord[op.version.key] = record
			if record.last.deleted {
				err = errors.New("failed to commit DELETE")
				goto unlock
			}
			if tx.ts < record.last.rTs {
				err = errors.New("failed to commit DELETE")
				goto unlock
			}
		}
	}

	// write-set -> wal
	if err := tx.SaveWal(); err != nil {
		log.Println(err)
	}

	// write-set -> db-memory
	for _, op := range sortedWriteSet {
		switch op.cmd {
		case INSERT:
			record := lockedRecord[op.version.key]
			record.last.deleted = false
		case UPDATE:
			record := lockedRecord[op.version.key]
			op.version.prev = record.last
			record.last = op.version
		case DELETE:
			record := lockedRecord[op.version.key]
			op.version.prev = record.last
			record.last = op.version
		}
	}

	tx.db.versionGC(&sortedWriteSet)

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

func (tx *Tx) SaveWal() error {
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
		return err
	}
	if err := tx.db.wALFile.Sync(); err != nil {
		return err
	}
	tx.db.walMu.Unlock()

	return nil
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
