package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChan := make(chan kafka.Event) // retorno do envio da mensagem
	producer := NewKafkaProducer()
	Publish("transferiu", "test", producer, []byte("transferencia"), deliveryChan)

	go DeliveryReport(deliveryChan) //async

	producer.Flush(5000)

	//ev := <-deliveryChan // sync
	//msg := ev.(*kafka.Message)
	//if msg.TopicPartition.Error != nil {
	//	fmt.Println("Erro ao enviar")
	//} else {
	//	fmt.Println("Mensagem enviada: ", msg.TopicPartition)
	//}
	//

}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "fc2-gokafka-kafka-1:9092",
		"delivery.timeout.ms": "0",     // infinito
		"acks":                "1",     // ack types -> 0 | 1 | all
		"enable.idempotence":  "false", // garante que a mensagem foi enviada e enviada apenas uma vez. Precisa ter "acks" :"all"
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatal(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny},
		Value: []byte(msg),
		Key:   key,
	}

	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return err
	}
	return nil

}

func DeliveryReport(deliveryChannel chan kafka.Event) {
	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
			}

		}
	}
}
