package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

const (
	DBFileName  = "seccampdb.db"
	WALFileName = "seccampdb.log"
	TmpFileName = "tmp.db"
)

func main() {
	fmt.Println("starting seccampdb...")

	db := NewDB(WALFileName, DBFileName)
	db.Setup()

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":7777")
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("admin >> ")
			if scanner.Scan() {
				input := strings.Fields(scanner.Text())
				if len(input) == 1 && input[0] == "exit" {
					db.Shutdown()
				}
			}
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		fmt.Println("--- new connection ---")
		go db.StartTx(conn)
	}
}
