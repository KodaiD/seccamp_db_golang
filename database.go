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
	CMD 	uint
	Record
}

type WriteSet []Operation

type Index map[string]string


type Tx struct {
	ID 	uint
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

func newTx(id uint, walFile *os.File, writeSet WriteSet, index Index) *Tx {
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

func (tx *Tx) SaveWal()  {
	// make redo log
	buf := make([]byte, 4096)
	idx := uint(0) // 書き込み開始位置

	for i := 0; i < len(tx.WriteSet); i++ {
		op := tx.WriteSet[i]
		checksum := crc32.ChecksumIEEE([]byte(op.Key))

		// serialize data
		size := serialize(buf, idx, op, checksum)
		idx += size
	}
	if _, err := tx.WalFile.Write(buf); err != nil {
		log.Fatal(err)
	}
	if err := tx.WalFile.Sync(); err != nil {
		log.Println("cannot sync wal-file")
	}
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
		size, op, checksum := deserialize(buf, idx)

		if checksum != crc32.ChecksumIEEE([]byte(op.Key)) {
			fmt.Println("load failed")
			continue
		}

		switch op.CMD {
		case INSERT:
			index[op.Key] = op.Value
		case UPDATE:
			index[op.Key] = op.Value
		case DELETE:
			delete(index, op.Key)
		}

		idx += size
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
	if err := tmpFile.Close(); err != nil {
		log.Println("cannot close tmp-file (DB-file)")
	}
	if err = os.Rename(TmpFileName, DbFileName); err != nil {
		log.Fatal(err)
	}
	if err := tmpFile.Sync(); err != nil {
		log.Println("cannot sync rename(tmp-file -> DB-file)")
	}
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
	if err := dbFile.Close(); err != nil {
		log.Println("cannot close DB-file")
	}
}

func clearFile(file *os.File) {
	if err := file.Truncate(0); err != nil {
		log.Println(err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		log.Println(err)
	}
	if err := file.Sync(); err != nil {
		log.Println(err)
	}
}