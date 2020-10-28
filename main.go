package main

import (
	"bufio"
	"fmt"
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

func main() {
	fmt.Println("starting db...")

	index := make(map[string]string)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("seccamp-db >> ")
		if scanner.Scan() {
			input := strings.Fields(scanner.Text())
			cmd := input[0]
			switch cmd {
			case "read":
				if len(input) != 2 {
					fmt.Println("wrong format -> read <key>")
				}
				key := input[1]
				if key == "*" {
					fmt.Println("key		| value")
					fmt.Println("----------------------------")
					for k, v := range index {
						fmt.Printf("%s		| %s\n", k, v)
					}
					fmt.Println("----------------------------")
				} else {
					value, exist := index[key]
					if !exist {
						fmt.Println("key not exists")
						continue
					}
					fmt.Println(value)
				}
			case "insert":
				if len(input) != 3 {
					fmt.Println("wrong format -> insert <key> <value>")
				}
				key, value := input[1], input[2]
				_, exist := index[key]
				if exist {
					fmt.Println("key already exists")
					continue
				}
				index[key] = value
			case "update":
				if len(input) != 3 {
					fmt.Println("wrong format -> update <key> <value>")
				}
				key, value := input[1], input[2]
				_, exist := index[key]
				if !exist {
					fmt.Println("key not exists")
					continue
				}
				index[key] = value
			case "delete":
				if len(input) != 2 {
					fmt.Println("wrong format -> delete <key>")
				}
				key := input[1]
				_, exist := index[key]
				if !exist {
					fmt.Println("key not exists")
					continue
				}
				delete(index, key)
			case "abort":
				os.Exit(0)
			default:
				fmt.Println("command not supported")
			}

		}
	}
}