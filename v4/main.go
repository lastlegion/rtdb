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

var redisAddr = flag.String("redis_addr", "localhost:6379", "Redis address")
var redisDB = flag.Int("redis_db", 0, "Redis database")
var redisPassword = flag.String("redis_password", "", "Redis password")
var redisChannel = flag.String("redis_broadcast_channel", "kv_broadcast", "Redis channel where events are published")
var clientId = flag.String("client_id", "client1", "ID for the client, make sure to provide unique ID for each client")

type KVStore struct {
	db          *bitcask.Bitcask
	redisClient redis.Client
	buffer      chan string
	initDone    chan bool
}

func NewKVStore(redisAddr string, redisPassword string, redisDB int, redisChannel string, clientId string) *KVStore {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	var dbFile = "/tmp/" + clientId
	bufferChan := make(chan string, 100)

	db, err := bitcask.Open(dbFile)

	if err != nil {
		log.Fatal("Failed to open db " + err.Error())
	}
	store := &KVStore{db: db,
		redisClient: *redisClient,
		buffer:      bufferChan,
		initDone:    make(chan bool),
	}
	go store.loadData()
	go store.subscribe()
	return store
}

func (k KVStore) loadData() {
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = k.redisClient.Scan(ctx, cursor, "*", 1).Result()
		if err != nil {
			log.Println("Failed to retrieve data ", err.Error())
		}
		for _, key := range keys {
			val, _ := k.redisClient.Get(ctx, key).Result()
			k.put(key, val)
		}
		if cursor == 0 {
			break
		}
	}
	k.initDone <- true
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

func (k KVStore) broadcastAndPersist(key string, value string) {
	publish_message := key + ":" + value
	k.redisClient.Publish(ctx, *redisChannel, publish_message)
	err := k.redisClient.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Println("Failed to persist " + err.Error())
	}
}

func (k KVStore) Put(key string, value string) {
	k.put(key, value)
	defer k.broadcastAndPersist(key, value)
}

func (k KVStore) subscribe() {
	pubsub := k.redisClient.Subscribe(ctx)
	pubsub.Subscribe(ctx, *redisChannel)

	go func() {
		// wait for initlization to complete before processing new events
		<-k.initDone
		for {
			msg := <-k.buffer
			payload := strings.Split(msg, ":")
			key, value := payload[0], payload[1]
			k.put(key, value)
		}
	}()
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error " + err.Error())
		}

		// if we're still at inital load, buffer messages
		k.buffer <- msg.Payload
	}

}

func main() {
	flag.Parse()
	s := *NewKVStore(*redisAddr, *redisPassword, *redisDB, *redisChannel, *clientId)

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
