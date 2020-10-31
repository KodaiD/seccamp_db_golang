package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"runtime"
	"strings"
)

/*
memo
-------------------------------------------------------------------
- wal は commit時のみ書き出すことにする(まず redo log, 次に commit log)
- checkpointing は起動時に限る
- write-set に追加で情報を含めてcommit時に redo log 生成する
- wal format
	- data(cmd, record(key, value))
	- size
	- key size
	- checksum(crc32 IEEE)
- write-set はデータが少なく、read時の検索などに時間がかからないと仮定
-------------------------------------------------------------------
 */

const (
	DbFileName  = "seccampdb.db"
	WalFileName = "seccampdb.log"
	TmpFileName = "tmp.db"
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




func main() {
	runtime.GOMAXPROCS(1) // single thread
	fmt.Println("starting seccampdb...")

	walFile, err := os.OpenFile(WalFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer walFile.Close()

	index := make(Index)
	var writeSet WriteSet

	// crash recovery (db file -> db-memory)
	dbFile, err := os.OpenFile(DbFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(dbFile)
	for scanner.Scan() {
		line := strings.Fields(scanner.Text())
		key := line[0]
		value := line[1]
		index[key] = value
		fmt.Println("recovering...")
	}
	dbFile.Close()

	// crash recovery (wal -> db-memory)
	buf := make([]byte, 4096)
	reader := bufio.NewReader(walFile)
	_, err = reader.Read(buf)
	if err != nil {
		log.Println("cannot do crash recovery")
	}

	idx := 0
	for buf[idx] != 0 {
		size := int(buf[idx])
		keySize := int(buf[idx+1])
		cmd := int(buf[idx+2])
		key := string(buf[idx+3:idx+3+keySize])
		value := string(buf[idx+3+keySize:idx+size-4])
		checksum := binary.BigEndian.Uint32(buf[idx+size-4:idx+size])

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

	// checkpointing (db-memory -> db file)
	tmpFile, err := os.Create(TmpFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer tmpFile.Close()
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
	if err = os.Remove(DbFileName); err != nil {
		log.Fatal(err)
	}
	if err = os.Rename(TmpFileName, DbFileName); err != nil {
		log.Fatal(err)
	}



	// main logic
	scanner = bufio.NewScanner(os.Stdin)
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

				exist := checkExistence(index, writeSet, key)
				if exist == "" {
					fmt.Println("key not exists")
				} else {
					fmt.Println(exist)
				}

			case "insert":
				if len(input) != 3 {
					fmt.Println("wrong format -> insert <key> <value>")
					continue
				}
				key := input[1]

				record := Record{key, input[2]}

				exist := checkExistence(index, writeSet, key)
				if exist != "" {
					fmt.Println("key already exists")
					continue
				}

				writeSet = append(writeSet, Operation{INSERT, record})

			case "update":
				if len(input) != 3 {
					fmt.Println("wrong format -> update <key> <value>")
					continue
				}
				key := input[1]

				record := Record{key, input[2]}

				exist := checkExistence(index, writeSet, key)
				if exist == "" {
					fmt.Println("key not exists")
					continue
				}
				writeSet = append(writeSet, Operation{UPDATE, record})

			case "delete":
				if len(input) != 2 {
					fmt.Println("wrong format -> delete <key>")
					continue
				}
				key := input[1]

				record := Record{key, "deleted"}

				exist := checkExistence(index, writeSet, key)
				if exist == "" {
					fmt.Println("key not exists")
					continue
				}
				writeSet = append(writeSet, Operation{DELETE, record})

			case "commit":
				// make redo log
				buf := make([]byte, 4096) // 4KiB page size にしとく
				idx := 0 // 書き込み開始位置
				for i := 0; i < len(writeSet); i++ {
					key := []byte(writeSet[i].Key)
					value := []byte(writeSet[i].Value)

					size := len(key) + len(value) + 7 // データ長は1byteで表せるものとする
					checksum := crc32.ChecksumIEEE(key)

					// serialize data
					buf[idx] = uint8(size)
					buf[idx+1] = uint8(len(key))
					buf[idx+2] = uint8(writeSet[i].CMD)
					copy(buf[idx+3:], key)
					copy(buf[idx+3+len(key):], value)
					binary.BigEndian.PutUint32(buf[idx+size-4:], checksum)

					idx += size
				}
				_, err := walFile.Write(buf)
				if err != nil {
					log.Fatal(err)
				}
				walFile.Sync()

				// refresh db-memory
				for i := 0; i < len(writeSet); i++ {
					op := writeSet[i]
					switch op.CMD {
					case INSERT:
						index[op.Key] = op.Value
					case UPDATE:
						index[op.Key] = op.Value
					case DELETE:
						delete(index, op.Key)
					}
				}

				// delete write-set
				writeSet = WriteSet{}

			case "abort":
				os.Exit(1)

			case "exit":
				fmt.Println("shut down...")

				// db-memory -> db file
				dbFile, _ = os.Create(DbFileName)
				for key, value := range index {
					line := key + " " + value + "\n"
					_, err := dbFile.WriteString(line)
					if err != nil {
						log.Println(err)
					}
				}
				os.Exit(0)

			case "all":
				readAll(index)

			default:
				fmt.Println("command not supported")
			}
			fmt.Println(writeSet)
		}
	}
}

// write-set から指定された key の record の存在を調べる
func checkExistence(index Index, writeSet WriteSet, key string) string {
	// check write-set
	for i := len(writeSet) - 1; 0 <= i; i-- {
		operation := writeSet[i]
		if key == operation.Record.Key {
			if operation.Record.Value == "deleted"{
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

// TODO: とりあえず保留
func readAll(index Index) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	for k, v := range index {
		fmt.Printf("%s		| %s\n", k, v)
	}
	fmt.Println("----------------------------")
}