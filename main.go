package main

import (
	"fmt"
	"log"
	"os"
	"sync"
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

	TestCase1 = "test_case_1.db"
	TestCase2 = "test_case_2.db"
)

func main() {
	fmt.Println("starting seccampdb...")

	db := NewDB(WALFileName, DBFileName)
	db.Setup()

	// open input file
	case1, err := os.Open(TestCase1)
	if err != nil {
		log.Fatal(err)
	}
	case2, err := os.Open(TestCase2)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	// 2 tx running
	wg.Add(1)
	go func() {
		defer wg.Done()
		db.StartTx(case1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		db.StartTx(case2)
	}()

	wg.Wait()
	db.Shutdown()
}