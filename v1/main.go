package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

var ctx = context.Background()

type KVStore struct {
	db map[string]string
}

func NewKVStore() *KVStore {
	store := &KVStore{db: map[string]string{}}
	return store
}

func (k KVStore) Get(key string) string {
	return k.db[key]
}

func (k KVStore) Put(key string, value string) {
	k.db[key] = value
}

func main() {
	s := *NewKVStore()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Real time db")
	flag := true
	for flag {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		input := strings.Split(strings.Trim(text, "\n"), " ")
		cmd := input[0]
		switch cmd {
		case "GET":
			key := input[1]
			value := s.Get(key)
			fmt.Println(value)
		case "PUT":
			key, value := input[1], input[2]
			s.Put(key, value)
			fmt.Println("Done...")
		case "EXIT":
			fmt.Println("EXIT...")
			flag = false
			break
		}
	}

}
