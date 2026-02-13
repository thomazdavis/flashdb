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
	fmt.Println("Commands: SET <key> <val> | GET <key> | DELETE <key> | FLUSH | LISTALL | EXIT")

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

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE <key>")
				continue
			}
			if err := db.Delete([]byte(parts[1])); err != nil {
				fmt.Printf("Error deleting: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "FLUSH":
			fmt.Println("Flushing memtable to disk...")
			if err := db.Flush(); err != nil {
				fmt.Printf("Error flushing: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "LISTALL":
			fmt.Println("\n\n--- STRATAGO FULL LAYER INSPECTION ---")

			// Active Memtable
			active := db.GetActiveContents()
			fmt.Printf("\n[Active Memtable] (%d keys)\n", len(active))
			for k, v := range active {
				fmt.Printf("  %s: \"%s\"\n", k, string(v))
			}

			// Immutable Memtable (Background Flush)
			immut := db.GetImmutableContents()
			if immut != nil {
				fmt.Printf("\n[Immutable Memtable] (%d keys)\n", len(immut))
				for k, v := range immut {
					fmt.Printf("  %s: \"%s\"\n", k, string(v))
				}
			}

			// WAL (On-disk)
			walData, _ := db.GetWAL().Recover()
			fmt.Printf("\n [WAL: %s] (%d entries)\n", db.GetWAL().Path(), len(walData))
			for k, v := range walData {
				fmt.Printf("  %s: \"%s\"\n", k, string(v))
			}

			// SSTables
			sstables := db.GetSSTableContents()
			for path, contents := range sstables {
				fmt.Printf("\n [SSTable: %s] (%d keys)\n", path, len(contents))
				for k, v := range contents {
					fmt.Printf("  %s: \"%s\"\n", k, string(v))
				}
			}

		case "PURGE":
			fmt.Print("Are you sure? This will delete ALL data. (y/n): ")
			if !scanner.Scan() {
				break
			}
			if strings.ToLower(scanner.Text()) == "y" {
				if err := db.Purge(); err != nil {
					fmt.Printf("Error purging DB: %v\n", err)
				} else {
					fmt.Println("Database purged. All data removed.")
				}
			} else {
				fmt.Println("Cancelled.")
			}

		case "EXIT":
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}
