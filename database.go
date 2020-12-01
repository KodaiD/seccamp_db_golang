package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

// supported operation
const (
	READ = 1 + iota
	INSERT
	UPDATE
	DELETE
	COMMIT
	ABORT
)

type DB struct {
	walMu       sync.Mutex
	wALFile     *os.File
	dBFile      *os.File
	index       sync.Map
	tsGenerator uint64
	aliveTx     AliveTx
}

type AliveTx struct {
	txs []uint64
	mu  sync.RWMutex
}

func NewDB(walFileName, dbFileName string) *DB {
	walFile, err := os.OpenFile(walFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	dbFile, err := os.OpenFile(dbFileName, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}

	return &DB{
		wALFile:     walFile,
		dBFile:      dbFile,
		index:       sync.Map{},
		tsGenerator: 0,
		aliveTx:     AliveTx{},
	}
}

func (db *DB) Shutdown() {
	fmt.Println("shut down...")

	// db-memory -> DB-file
	db.saveData()
	// clear wal-file
	db.clearFile()
	if err := db.wALFile.Close(); err != nil {
		log.Println(err)
	}
	if err := db.dBFile.Close(); err != nil {
		log.Println(err)
	}

	os.Exit(0)
}

func (db *DB) Setup() {
	// crash recovery (db-file -> db-memory)
	db.loadData()

	// crash recovery (wal-file -> db-memory)
	db.loadWal()

	// checkpointing (db-memory -> db-file)
	db.saveData()

	// clear log-file
	db.clearFile()
}

func (db *DB) StartTx(conn net.Conn) {
	tx := NewTx(db)
	scanner := bufio.NewScanner(conn)
	for {
		conn.Write([]byte("seccampdb >> "))
		if scanner.Scan() {
			input := strings.Fields(scanner.Text())
			cmd := input[0]
			switch cmd {
			case "read":
				if len(input) != 2 {
					conn.Write([]byte("wrong format -> read <key>\n"))
					continue
				}
				key := input[1]
				value, err := tx.Read(key)
				if err != nil {
					conn.Write([]byte(err.Error()+"\n"))

				} else {
					conn.Write([]byte(value+"\n"))
				}
			case "insert":
				if len(input) != 3 {
					conn.Write([]byte("wrong format -> insert <key> <value>\n"))
					continue
				}
				key := input[1]
				value := input[2]
				if err := tx.Insert(key, value); err != nil {
					conn.Write([]byte(err.Error()+"\n"))
				}
			case "update":
				if len(input) != 3 {
					conn.Write([]byte("wrong format -> update <key> <value>\n"))
					continue
				}
				key := input[1]
				value := input[2]
				if err := tx.Update(key, value); err != nil {
					conn.Write([]byte(err.Error()+"\n"))
				}
			case "delete":
				if len(input) != 2 {
					conn.Write([]byte("wrong format -> delete <key>\n"))
					continue
				}
				key := input[1]
				if err := tx.Delete(key); err != nil {
					conn.Write([]byte(err.Error()+"\n"))
				}
			case "commit":
				if err := tx.Commit(); err != nil {
					conn.Write([]byte(err.Error()+"\n"))
				}
				conn.Write([]byte("committed\n"))
				fmt.Println("committed")
				conn.Close()
				return
			case "abort":
				tx.DestructTx()
				conn.Write([]byte("aborted\n"))
				fmt.Println("aborted")
				conn.Close()
				return
			case "all":
				readAll(&db.index) // TODO:
			default:
				conn.Write([]byte("command not supported\n"))
			}
		}
	}
}

func (db *DB) loadWal() {
	reader := bufio.NewReader(db.wALFile)
	for {
		buf := make([]byte, 4096)

		if _, err := reader.Read(buf); err != nil {
			if err == io.EOF { // 全て読み終わった
				break
			}
			log.Println("cannot do crash recovery")
			continue // 次の4KiBを読みにいく
		}

		idx := uint(0)
		for buf[idx] != 0 {
			size, op, checksum := deserialize(buf, idx)

			if checksum != crc32.ChecksumIEEE([]byte(op.version.key)) {
				fmt.Println("load failed")
				continue
			}

			switch op.cmd {
			case INSERT:
				record := Record{
					key:   op.version.key,
					last:  op.version,
				}
				db.index.Store(op.version.key, &record)
			case UPDATE:
				record := Record{
					key:   op.version.key,
					last:  op.version,
				}
				db.index.Store(op.version.key, &record)
			case DELETE:
				db.index.Delete(op.version.key)
			}
			idx += size
		}
	}
}

func serialize(buf []byte, idx uint, op *Operation, checksum uint32) uint {
	size := uint(len(op.version.key) + len(op.version.value) + 7)
	buf[idx] = uint8(size)
	buf[idx+1] = uint8(len(op.version.key))
	buf[idx+2] = op.cmd
	copy(buf[idx+3:], op.version.key)
	copy(buf[idx+3+uint(len(op.version.key)):], op.version.value)
	binary.BigEndian.PutUint32(buf[idx+size-4:], checksum)

	return size
}

func deserialize(buf []byte, idx uint) (uint, *Operation, uint32) {
	size := uint(buf[idx])
	keySize := uint(buf[idx+1])
	cmd := buf[idx+2]
	key := string(buf[idx+3 : idx+3+keySize])
	value := string(buf[idx+3+keySize : idx+size-4])
	checksum := binary.BigEndian.Uint32(buf[idx+size-4 : idx+size])

	op := &Operation{
		cmd: cmd,
		version: &Version{
			key:   key,
			value: value,
			wTs:   0,
			rTs:   0,
			prev:  nil,
		},
	}

	return size, op, checksum
}

func (db *DB) saveData() {
	tmpFile, err := os.Create(TmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	db.index.Range(func(k, v interface{}) bool {
		key := k.(string)
		record := v.(*Record)
		line := key + " " + record.last.value + "\n"
		_, err := tmpFile.WriteString(line)
		if err != nil {
			log.Println(err)
		}
		return true
	})
	if err = tmpFile.Sync(); err != nil {
		log.Fatal(err)
	}
	if err = os.Rename(TmpFileName, DBFileName); err != nil {
		log.Fatal(err)
	}
	if err := tmpFile.Sync(); err != nil {
		log.Println(err)
	}
	if err := db.dBFile.Close(); err != nil {
		log.Println(err)
	}
	db.dBFile = tmpFile
}

func (db *DB) loadData() {
	scanner := bufio.NewScanner(db.dBFile)
	for scanner.Scan() {
		line := strings.Fields(scanner.Text())
		if len(line) != 2 {
			fmt.Println("broken data")
		}
		key := line[0]
		value := line[1]
		version := &Version{
			key:   key,
			value: value,
			wTs:   0,
			rTs:   0,
			prev:  nil,
		}
		db.index.Store(key, &Record{
			key:   key,
			last:  version,
			mu:    sync.Mutex{},
		})
		// fmt.Println("recovering...")
	}
}

func (db *DB) clearFile() {
	if err := db.wALFile.Truncate(0); err != nil {
		log.Println(err)
	}
	if _, err := db.wALFile.Seek(0, 0); err != nil {
		log.Println(err)
	}
	if err := db.wALFile.Sync(); err != nil {
		log.Println(err)
	}
}

func (db *DB) versionGC(sortedWriteSet *[]*Operation) {
	if len(*sortedWriteSet) > 0 {
		// 起動中のtxのtsが入った配列の最小値をとってくる
		db.aliveTx.mu.RLock()
		min := db.aliveTx.txs[0]
		for _, ts := range db.aliveTx.txs {
			if ts < min {
				min = ts
			}
		}
		db.aliveTx.mu.RUnlock()

		// それより小さいversionを除去(対象は、version追加したとことか)
		var prev *Operation
		for _, op := range *sortedWriteSet {
			if prev != nil && prev.version.key == op.version.key {
				continue
			}
			cur := op.version
			if cur.prev != nil {
				for cur != nil && min <= cur.wTs {
					cur = cur.prev
				}
				if cur != nil {
					cur.prev = nil
				}
			}
			prev = op
		}
	}
}