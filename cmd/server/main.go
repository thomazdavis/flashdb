package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/thomazdavis/stratago"
)

func main() {
	db, err := stratago.Open("./data")
	if err != nil {
		fmt.Printf("Error opening DB: %v\n", err)
		return
	}
	defer db.Close()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("StrataGo Shell")
	fmt.Println("Commands: SET <key> <val> | GET <key> | FLUSH | EXIT")

	for {
		fmt.Print("stratago> ")
		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)
		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])
		switch command {
		case "SET":
			if len(parts) < 3 {
				fmt.Println("Usage: SET <key> <val>")
				continue
			}
			db.Put([]byte(parts[1]), []byte(parts[2]))
			fmt.Println("OK")

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			val, found := db.Get([]byte(parts[1]))
			if found {
				fmt.Printf("\"%s\"\n", string(val))
			} else {
				fmt.Println("(nil)")
			}

		case "FLUSH":
			fmt.Println("Flushing memtable to disk...")
			if err := db.Flush(); err != nil {
				fmt.Printf("Error flushing: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "EXIT":
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
