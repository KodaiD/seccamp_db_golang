package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

type Record struct {
	Key 	string
	Value 	string
}

type Operation struct {
	CMD 	int
	Record
}

type WriteSet []Operation

type Index map[string]string


type Tx struct {
	ID 	int
	WalFile 	*os.File
	WriteSet 	WriteSet
	Index 		Index // 共有
}

type TxLogic interface {
	Read(key string)
	Insert(key string, value string)
	Update(key string, value string)
	Delete(key string)
	Commit()
	Abort()
	SaveWal()
}

func newTx(id int, walFile *os.File, writeSet WriteSet, index Index) *Tx {
	return &Tx{
		ID:      	id,
		WalFile: 	walFile,
		WriteSet: 	writeSet,
		Index: 		index,
	}
}

func (tx *Tx) Read(key string)  {
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist == "" {
		fmt.Println("key not exists")
	} else {
		fmt.Println(exist)
	}
}

func (tx *Tx) Insert(key, value string) {
	record := Record{key, value}
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist != "" {
		fmt.Println("key already exists")
		return
	}
	tx.WriteSet = append(tx.WriteSet, Operation{INSERT, record})
}

func (tx *Tx) Update(key, value string) {
	record := Record{key, value}
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist == "" {
		fmt.Println("key not exists")
		return
	}
	tx.WriteSet = append(tx.WriteSet, Operation{UPDATE, record})
}

func (tx *Tx) Delete(key string) {
	record := Record{key, "deleted"}
	exist := checkExistence(tx.Index, tx.WriteSet, key)
	if exist == "" {
		fmt.Println("key not exists")
		return
	}
	tx.WriteSet = append(tx.WriteSet, Operation{DELETE, record})
}

func (tx *Tx) Commit() {
	// write-set -> wal
	tx.SaveWal()

	// write-set -> db-memory
	for i := 0; i < len(tx.WriteSet); i++ {
		op := tx.WriteSet[i]
		switch op.CMD {
		case INSERT:
			tx.Index[op.Key] = op.Value
		case UPDATE:
			tx.Index[op.Key] = op.Value
		case DELETE:
			delete(tx.Index, op.Key)
		}
	}
	// delete write-set
	tx.WriteSet = WriteSet{}
}

func (tx *Tx) Abort() {
	os.Exit(1)
}

func (tx *Tx) SaveWal()  {
	// make redo log
	buf := make([]byte, 4096)
	idx := 0 // 書き込み開始位置

	for i := 0; i < len(tx.WriteSet); i++ {
		key := []byte(tx.WriteSet[i].Key)
		value := []byte(tx.WriteSet[i].Value)
		size := len(key) + len(value) + 7
		checksum := crc32.ChecksumIEEE(key)

		/*
			wal format
			=========================
			- size(total size) 1 byte
			- key size         1 byte
			- data(key)        ? byte
			- data(value)      ? byte
			- checksum         4 byte
			=========================
		*/

		// serialize data
		buf[idx] = uint8(size)
		buf[idx+1] = uint8(len(key))
		buf[idx+2] = uint8(tx.WriteSet[i].CMD)
		copy(buf[idx+3:], key)
		copy(buf[idx+3+len(key):], value)
		binary.BigEndian.PutUint32(buf[idx+size-4:], checksum)

		idx += size
	}
	_, err := tx.WalFile.Write(buf)
	if err != nil {
		log.Fatal(err)
	}
	tx.WalFile.Sync()
}

// write-set から指定された key の record の存在を調べる
func checkExistence(index Index, writeSet WriteSet, key string) string {
	// check write-set
	for i := len(writeSet) - 1; 0 <= i; i-- {
		operation := writeSet[i]
		if key == operation.Record.Key {
			if operation.CMD == DELETE {
				return ""
			}
			return operation.Record.Value
		}
	}
	// check index
	value, exist := index[key]
	if !exist {
		return ""
	}
	return value
}

// read all data in db-memory
func readAll(index Index) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	for k, v := range index {
		fmt.Printf("%s		| %s\n", k, v)
	}
	fmt.Println("----------------------------")
}

func loadData(index Index) {
	dbFile, err := os.OpenFile(DbFileName, os.O_CREATE|os.O_RDONLY, 0666)
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
		index[key] = value
		fmt.Println("recovering...")
	}
	dbFile.Close()
}

func loadWal(index Index, walFile *os.File) {
	buf := make([]byte, 4096) // TODO: 小さい
	reader := bufio.NewReader(walFile)
	_, err := reader.Read(buf)
	if err != nil {
		log.Println("cannot do crash recovery")
	}

	idx := uint(0)
	for buf[idx] != 0 {
		size := uint(buf[idx])
		keySize := uint(buf[idx+1])
		cmd := uint(buf[idx+2])
		key := string(buf[idx+3 : idx+3+keySize])
		value := string(buf[idx+3+keySize : idx+size-4])
		checksum := binary.BigEndian.Uint32(buf[idx+size-4 : idx+size])

		if checksum != crc32.ChecksumIEEE([]byte(key)) {
			fmt.Println("load failed")
			continue
		}

		switch cmd {
		case INSERT:
			index[key] = value
		case UPDATE:
			index[key] = value
		case DELETE:
			delete(index, key)
		}

		idx += size
	}
}

func saveData(index Index) {
	tmpFile, err := os.Create(TmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	for key, value := range index {
		line := key + " " + value + "\n"
		_, err := tmpFile.WriteString(line)
		if err != nil {
			log.Println(err)
		}
	}
	if err = tmpFile.Sync(); err != nil {
		log.Fatal(err)
	}
	tmpFile.Close()
	if err = os.Rename(TmpFileName, DbFileName); err != nil {
		log.Fatal(err)
	}
	tmpFile.Sync()
}