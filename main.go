package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

var redisAddr = flag.String("redis_addr", "localhost:6379", "Redis address")
var redisDB = flag.Int("redis_db", 0, "Redis database")
var redisPassword = flag.String("redis_password", "", "Redis password")
var redisChannel = flag.String("redis_broadcast_channel", "kv_broadcast", "Redis channel where events are published")
var bufferSize = flag.Int64("buffer_size", 1000, "Buffer size for holding messages while cold starting")
var clientId = flag.String("client_id", "client1", "ID for the client, make sure to provide unique ID for each client")

func main() {
	flag.Parse()
	s := NewStore(*redisAddr, *redisPassword, *redisDB, *redisChannel, *clientId, *bufferSize)

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
