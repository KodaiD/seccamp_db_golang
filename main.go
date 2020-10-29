package main

import (
	"bufio"
	"fmt"
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
	- checksum(crc32)
	- size
	- data(cmd, record(key, value))
- write-set はデータが少なく、read時の検索などに時間がかからないと仮定
-------------------------------------------------------------------
 */

const (
	DbFile = "db.csv"
	WalFile = "db.log"
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

//type RedoLog struct {
//	checksum int
//	size int
//	record Record
//}

func main() {
	runtime.GOMAXPROCS(1) // single thread
	fmt.Println("starting db...")

	index := make(Index)
	var writeSet WriteSet

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("seccamp-db >> ")
		if scanner.Scan() {
			input := strings.Fields(scanner.Text())
			if len(input) < 2 {
				fmt.Println("not enough arguments")
				continue
			}
			cmd := input[0]
			key := input[1]

			switch cmd {
			case "read":
				if len(input) != 2 {
					fmt.Println("wrong format -> read <key>")
					continue
				}

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

				record := Record{key, "deleted"}

				exist := checkExistence(index, writeSet, key)
				if exist == "" {
					fmt.Println("key not exists")
					continue
				}
				writeSet = append(writeSet, Operation{DELETE, record})

			case "commit":
				//
			case "abort":
				os.Exit(0)
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

// とりあえず保留
func readAll(index *Index) {
	fmt.Println("key		| value")
	fmt.Println("----------------------------")
	for k, v := range *index {
		fmt.Printf("%s		| %s\n", k, v)
	}
	fmt.Println("----------------------------")
}