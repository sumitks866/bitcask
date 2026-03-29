package main

import (
	"fmt"

	"github.com/sumitks866/bitcask"
)

func main() {
	fmt.Println("Bitcask Database Server Starting...")

	// Initialize the database
	bitcask.NewDB()

	fmt.Println("Key-value pair stored successfully")

}
