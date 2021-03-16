package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/prologic/bitcask"
)

var ctx = context.Background()

const redisAddr = "localhost:6379"
const redisDB = 0
const redisPassword = ""
const redisChannel = "kv_broadcast"

var clientId = flag.String("client_id", "client1", "Client ID for this client")

type KVStore struct {
	db          *bitcask.Bitcask
	redisClient redis.Client
}

func NewKVStore() *KVStore {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	var dbFile = "/tmp/" + *clientId

	db, err := bitcask.Open(dbFile)

	if err != nil {
		log.Fatal("Failed to open db " + err.Error())
	}
	store := &KVStore{db: db,
		redisClient: *redisClient,
	}
	go store.subscribe()
	return store
}

func (k KVStore) Get(key string) string {
	val, err := k.db.Get([]byte(key))
	if err != nil {
		log.Printf("Failed to get message " + err.Error())
	}
	return string(val)
}

func (k KVStore) put(key string, value string) error {
	err := k.db.Put([]byte(key), []byte(value))
	if err != nil {
		log.Printf("Failed to write message " + err.Error())
		return err
	}
	return nil
}

func (k KVStore) Put(key string, value string) {
	k.put(key, value)
	publish_message := key + ":" + value
	k.redisClient.Publish(ctx, redisChannel, publish_message)
}

func (k KVStore) subscribe() {
	pubsub := k.redisClient.Subscribe(ctx)
	pubsub.Subscribe(ctx, redisChannel)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error " + err.Error())
		}
		payload := strings.Split(msg.Payload, ":")
		key, value := payload[0], payload[1]
		k.put(key, value)
	}

}

func main() {
	flag.Parse()

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
