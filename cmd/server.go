package main

import (
	"fmt"
	"log"

	"github.com/sumitks866/bitcask"
)

func main() {
	fmt.Println("Bitcask Database Server Starting...")

	// Initialize the database
	db, err := bitcask.NewDB()
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	fmt.Println("Key-value pair stored successfully")

}
