package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
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
	walMu   sync.Mutex
	wALFile *os.File
	dBFile  *os.File
	index   sync.Map
	n       uint64
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
		wALFile: walFile,
		dBFile:  dbFile,
		index:   sync.Map{},
		n: 		 0,
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

func (db *DB) StartTx(reader io.Reader) {
	tx := NewTx(db)
	scanner := bufio.NewScanner(reader)
	for {
		fmt.Print("seccampdb >> ")
		if scanner.Scan() {
			input := strings.Fields(scanner.Text())
			cmd := input[0]
			switch cmd {
			case "read":
				if len(input) != 2 {
					fmt.Println("wrong format -> read <key>")
					continue
				}
				key := input[1]
				value, err := tx.Read(key)
				if err != nil {
					log.Println(err)
				}
				fmt.Println(value)
			case "insert":
				if len(input) != 3 {
					fmt.Println("wrong format -> insert <key> <value>")
					continue
				}
				key := input[1]
				value := input[2]
				if err := tx.Insert(key, value); err != nil {
					log.Println(err)
				}
			case "update":
				if len(input) != 3 {
					fmt.Println("wrong format -> update <key> <value>")
					continue
				}
				key := input[1]
				value := input[2]
				if err := tx.Update(key, value); err != nil {
					log.Println(err)
				}
			case "delete":
				if len(input) != 2 {
					fmt.Println("wrong format -> delete <key>")
					continue
				}
				key := input[1]
				if err := tx.Delete(key); err != nil {
					log.Println(err)
				}
			case "commit":
				if err := tx.Commit(); err != nil {
					log.Println(err)
				}
			case "abort":
				tx.DestructTx()
				return
			case "all":
				readAll(&db.index) // TODO:
			default:
				fmt.Println("command not supported")
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
					first: op.version,
					last:  op.version,
				}
				db.index.Store(op.version.key, record)
			case UPDATE:
				record := Record{
					key:   op.version.key,
					first: op.version,
					last:  op.version,
				}
				db.index.Store(op.version.key, record)
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
	buf[idx+2] = uint8(op.cmd)
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
		cmd:     cmd,
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
	fmt.Println("start----------------")
	tmpFile, err := os.Create(TmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	db.index.Range(func(k, v interface{}) bool {
		key := k.(string)
		record := v.(Record)
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
		db.index.Store(key, Record{
			key:   key,
			first: version,
			last:  version,
		})
		fmt.Println("recovering...")
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