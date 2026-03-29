package main

import (
	"fmt"

	"github.com/sumitks866/bitcask"
)

func main() {
	fmt.Println("Bitcask Database Server Starting...")

	// Initialize the database
	db := bitcask.NewDB()

	//multiple put operations
	err := db.Put([]byte("name"), []byte("Bitcask"))
	if err != nil {
		fmt.Printf("Error putting key-value pair: %v\n", err)
		return
	}

	err = db.Put([]byte("version"), []byte("1.0"))
	if err != nil {
		fmt.Printf("Error putting key-value pair: %v\n", err)
		return
	}

	err = db.Put([]byte("author"), []byte("Sumit Singh"))
	if err != nil {
		fmt.Printf("Error putting key-value pair: %v\n", err)
		return
	}

	fmt.Println("\n\n\n-------- Reading --------\n\n\n")

	// multiple get operations
	value, err := db.Get([]byte("name"))
	if err != nil {
		fmt.Printf("Error getting value for key 'name': %v\n", err)
	} else {
		fmt.Printf("Value for key 'name': %s\n", string(value))
	}

	value, err = db.Get([]byte("version"))
	if err != nil {
		fmt.Printf("Error getting value for key 'version': %v\n", err)
	} else {
		fmt.Printf("Value for key 'version': %s\n", string(value))
	}

	value, err = db.Get([]byte("author"))
	if err != nil {
		fmt.Printf("Error getting value for key 'author': %v\n", err)
	} else {
		fmt.Printf("Value for key 'author': %s\n", string(value))
	}

	fmt.Println("Key-value pair stored successfully")

}
