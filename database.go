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

	"github.com/KodaiD/rwumutex"
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
	index   Index
}

type Index map[string]Record


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
		index:   make(Index),
	}
}

func (db *DB) Shutdown() {
	fmt.Println("shut down...")

	// batch 物理 delete
	for _, record := range db.index {
		if record.deleted {
			delete(db.index, record.key)
		}
	}

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
	tx := NewTx(1, db) // TODO: unique id
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
				if err := tx.Read(key); err != nil {
					log.Println(err)
				}
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
				tx.Commit()
			case "abort":
				tx.writeSet = make(WriteSet)
				tx.readSet = make(ReadSet)
				tx = NewTx(1, db)
			case "exit":
				return
			case "all":
				readAll(db.index) // TODO:
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

			if checksum != crc32.ChecksumIEEE([]byte(op.record.key)) {
				fmt.Println("load failed")
				continue
			}

			switch op.cmd {
			case INSERT:
				db.index[op.record.key] = *op.record
			case UPDATE:
				db.index[op.record.key] = *op.record
			case DELETE:
				delete(db.index, op.record.key)
			}

			idx += size
		}
	}
}

func serialize(buf []byte, idx uint, op *Operation, checksum uint32) uint {
	size := uint(len(op.record.key) + len(op.record.value) + 7)
	buf[idx] = uint8(size)
	buf[idx+1] = uint8(len(op.record.key))
	buf[idx+2] = uint8(op.cmd)
	copy(buf[idx+3:], op.record.key)
	copy(buf[idx+3+uint(len(op.record.key)):], op.record.value)
	binary.BigEndian.PutUint32(buf[idx+size-4:], checksum)

	return size
}

func deserialize(buf []byte, idx uint) (uint, *Operation, uint32) {
	size := uint(buf[idx])
	keySize := uint(buf[idx+1])
	cmd := uint(buf[idx+2])
	key := string(buf[idx+3 : idx+3+keySize])
	value := string(buf[idx+3+keySize : idx+size-4])
	checksum := binary.BigEndian.Uint32(buf[idx+size-4 : idx+size])

	op := &Operation{
		cmd:    cmd,
		record: &Record{key, value, new(rwuMutex.RWUMutex), false},
	}

	return size, op, checksum
}

func (db *DB) saveData() {
	fmt.Println("start----------------")
	tmpFile, err := os.Create(TmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	for key, record := range db.index {
		line := key + " " + record.value + "\n"
		_, err := tmpFile.WriteString(line)
		if err != nil {
			log.Println(err)
		}
	}
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
		db.index[key] = Record{key, value, new(rwuMutex.RWUMutex), false}
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