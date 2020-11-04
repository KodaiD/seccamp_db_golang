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
			=========================
			- size(total size) 1 byte
			- key size         1 byte
			- data(key)        ? byte
			- data(value)      ? byte
			- checksum         4 byte
			=========================
- write-set はデータが少なく、read 時の検索などに時間がかからないと仮定
- wal size は 4KiB (page size) にしとく
- wal に記録する 1 record あたりのデータ長は 1 byte で表せるものとする
-------------------------------------------------------------------
 */

const (
	DBFileName  = "seccampdb.db"
	WALFileName = "seccampdb.log"
	TmpFileName = "tmp.db"
)

func main() {
	runtime.GOMAXPROCS(1) // single thread
	fmt.Println("starting seccampdb...")

	db := NewDB()
	db.Setup()

	tx := NewTx(1, db.WALFile, db.Index)
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
				tx.Abort()

			case "exit":
				db.Shutdown()

			case "all":
				readAll(db.Index)

			default:
				fmt.Println("command not supported")
			}
		}
	}
}