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
	WALFile *os.File
	DBFile  *os.File
	index   Index
}

type Index map[string]string

func NewDB() *DB {
	walFile, err := os.OpenFile(WALFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer walFile.Close()

	return &DB{
		WALFile: walFile,
		DBFile:  nil,
		index:   make(Index),
	}
}

func (db *DB) Shutdown() {
	fmt.Println("shut down...")
	// db-memory -> DB-file
	db.saveData()
	// clear wal-file
	db.clearFile()

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

func (db *DB) loadWal() {
	reader := bufio.NewReader(db.WALFile)
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

			if checksum != crc32.ChecksumIEEE([]byte(op.Key)) {
				fmt.Println("load failed")
				continue
			}

			switch op.CMD {
			case INSERT:
				db.index[op.Key] = op.Value
			case UPDATE:
				db.index[op.Key] = op.Value
			case DELETE:
				delete(db.index, op.Key)
			}

			idx += size
		}
	}
}

func serialize(buf []byte, idx uint, op Operation, checksum uint32) uint {
	size := uint(len(op.Key) + len(op.Value) + 7)
	buf[idx] = uint8(size)
	buf[idx+1] = uint8(len(op.Key))
	buf[idx+2] = uint8(op.CMD)
	copy(buf[idx+3:], op.Key)
	copy(buf[idx+3+uint(len(op.Key)):], op.Value)
	binary.BigEndian.PutUint32(buf[idx+size-4:], checksum)

	return size
}

func deserialize(buf []byte, idx uint) (uint, Operation, uint32) {
	size := uint(buf[idx])
	keySize := uint(buf[idx+1])
	cmd := uint(buf[idx+2])
	key := string(buf[idx+3 : idx+3+keySize])
	value := string(buf[idx+3+keySize : idx+size-4])
	checksum := binary.BigEndian.Uint32(buf[idx+size-4 : idx+size])

	op := Operation{
		CMD:    cmd,
		Record: Record{key, value},
	}

	return size, op, checksum
}

func (db *DB) saveData() {
	tmpFile, err := os.Create(TmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	for key, value := range db.index {
		line := key + " " + value + "\n"
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
	if err := tmpFile.Close(); err != nil {
		log.Println(err)
	}
}

func (db *DB) loadData() {
	dbFile, err := os.OpenFile(DBFileName, os.O_CREATE|os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(dbFile)
	for scanner.Scan() {
		line := strings.Fields(scanner.Text())
		if len(line) != 2 {
			fmt.Println("broken data")
		}
		key := line[0]
		value := line[1]
		db.index[key] = value
		fmt.Println("recovering...")
	}
	if err := dbFile.Close(); err != nil {
		log.Println("cannot close DB-file")
	}
}

func (db *DB) clearFile() {
	if err := db.WALFile.Truncate(0); err != nil {
		log.Println(err)
	}
	if _, err := db.WALFile.Seek(0, 0); err != nil {
		log.Println(err)
	}
	if err := db.WALFile.Sync(); err != nil {
		log.Println(err)
	}
}