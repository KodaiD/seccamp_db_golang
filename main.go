package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
)

/*
memo
-------------------------------------------------------------------
- wal は commit 時のみ書き出すことにする(まず redo log, 次に commit log)
- checkpointing は起動時に限る
- write-set に追加で情報を含めて commit 時に redo log 生成する
- wal format
	- data (cmd, record(key, value))
	- size
	- key size
	- checksum (crc32 IEEE)
- write-set はデータが少なく、read 時の検索などに時間がかからないと仮定
- wal size は 4KiB (page size) にしとく
- wal に記録する 1 record あたりのデータ長は 1 byte で表せるものとする
-------------------------------------------------------------------
 */

const (
	DbFileName  = "seccampdb.db"
	WalFileName = "seccampdb.log"
	TmpFileName = "tmp.db"
)

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
	loadData(index)

	// crash recovery (wal -> db-memory)
	loadWal(index, walFile)

	// checkpointing (db-memory -> db file)
	saveData(index)

	tx := newTx(1, walFile, writeSet, index)
	// main logic
	scanner := bufio.NewScanner(os.Stdin)
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
				tx.Read(key)

			case "insert":
				if len(input) != 3 {
					fmt.Println("wrong format -> insert <key> <value>")
					continue
				}
				key := input[1]
				value := input[2]
				tx.Insert(key, value)

			case "update":
				if len(input) != 3 {
					fmt.Println("wrong format -> update <key> <value>")
					continue
				}
				key := input[1]
				value := input[2]
				tx.Update(key, value)

			case "delete":
				if len(input) != 2 {
					fmt.Println("wrong format -> delete <key>")
					continue
				}
				key := input[1]
				tx.Delete(key)

			case "commit":
				tx.Commit()

			case "abort":
				tx.Abort()

			case "exit":
				fmt.Println("shut down...")
				// db-memory -> db file
				saveData(index)
				os.Exit(0)

			case "all":
				readAll(index)

			default:
				fmt.Println("command not supported")
			}
		}
	}
}