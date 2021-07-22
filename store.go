package main

import (
	"log"
	"strings"

	"github.com/go-redis/redis"
	"git.mills.io/prologic/bitcask"
)

type Store struct {
	db          *bitcask.Bitcask
	redisClient redis.Client
	buffer      chan string
	initDone    chan bool
	bufferSize  int64
}

func NewStore(redisAddr string, redisPassword string, redisDB int, redisChannel string, clientId string, bufferSize int64) *Store {

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
	store := &Store{db: db,
		redisClient: *redisClient,
		buffer:      bufferChan,
		initDone:    make(chan bool),
		bufferSize:  bufferSize,
	}
	go store.loadData()
	go store.subscribe()
	return store
}

func (k Store) loadData() {
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = k.redisClient.Scan(cursor, "*", k.bufferSize).Result()
		if err != nil {
			log.Println("Failed to retrieve data ", err.Error())
		}
		for _, key := range keys {
			val, _ := k.redisClient.Get(key).Result()
			k.put(key, val)
		}
		if cursor == 0 {
			break
		}
	}
	k.initDone <- true
}

func (k Store) Get(key string) string {
	val, err := k.db.Get([]byte(key))
	if err != nil {
		log.Printf("Failed to get message " + err.Error())
	}
	return string(val)
}

func (k Store) put(key string, value string) error {
	err := k.db.Put([]byte(key), []byte(value))
	if err != nil {
		log.Printf("Failed to write message " + err.Error())
		return err
	}
	return nil
}

func (k Store) broadcastAndPersist(key string, value string) {
	publish_message := key + ":" + value
	k.redisClient.Publish(*redisChannel, publish_message)
	err := k.redisClient.Set(key, value, 0).Err()
	if err != nil {
		log.Println("Failed to persist " + err.Error())
	}
}

func (k Store) Put(key string, value string) {
	k.put(key, value)
	defer k.broadcastAndPersist(key, value)
}

func (k Store) subscribe() {
	pubsub := k.redisClient.Subscribe()
	pubsub.Subscribe(*redisChannel)

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
		msg, err := pubsub.ReceiveMessage()
		if err != nil {
			log.Printf("Error " + err.Error())
		}

		// if we're still at inital load, buffer messages
		k.buffer <- msg.Payload
	}

}
