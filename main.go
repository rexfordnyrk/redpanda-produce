package main

import (
	"encoding/json"
	"fmt"
 	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"time"
)

type mytype []map[string]interface{}
var data mytype

func init() {
		file, err := ioutil.ReadFile("delivery_requests.json")
		if err != nil {
			log.Fatal(err)
		}
		err = json.Unmarshal(file, &data)
		if err != nil {
			log.Fatal(err)
		}
}
/*
	Initialize NewConfig configuration sarama.NewConfig
	Create producer sarama.NewSyncProducer
	Create message sarama.ProducerMessage
	Send message producer.SendMessage
*/
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	msg := &sarama.ProducerMessage{}
	msg.Topic = "delivery-requests"

	producer, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("producer close, err:", err)
		return
	}

	defer producer.Close()

	for _, record := range data{
		rec, _ := json.Marshal(record)
		msg.Value = sarama.StringEncoder(rec)
		pid, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println("send message failed,", err)
			panic(err)
		}
		fmt.Printf("pid: %v, offset: %v, topic: %v, key: %v, msg: %v\n", pid, offset, msg.Topic, msg.Key, msg.Value)
		time.Sleep(time.Second * 3)
	}

}
