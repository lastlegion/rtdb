package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type KVStore struct {
	db          map[string]string
	redisClient redis.Client
}

func NewKVStore() *KVStore {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	store := &KVStore{db: map[string]string{},
		redisClient: *redisClient,
	}
	go store.update(10)
	return store
}

func (k KVStore) Get(key string) string {
	return k.db[key]
}

func (k KVStore) Put(key string, value string) {
	k.db[key] = value
	k.redisClient.Publish(ctx, "kv_broadcast2", key+":"+value)
}

func (k KVStore) update(value int) {
	pubsub := k.redisClient.Subscribe(ctx)
	pubsub.Subscribe(ctx, "kv_broadcast2")

	for {
		msg, _ := pubsub.ReceiveMessage(ctx)
		log.Printf(msg.Payload)
		payload := strings.Split(msg.Payload, ":")
		key, value := payload[0], payload[1]
		k.db[key] = value
	}
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
