package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
)

const (
	SensorMaxID = 2
	ValueMax    = 200
	TypeMaxID   = 1
)

type SensorData struct {
	SensorID int64
	Value    float64
	Type     int32
}

func NewSensorData() SensorData {
	return SensorData{
		SensorID: rand.Int63n(SensorMaxID),
		Value:    rand.NormFloat64() * ValueMax,
		Type:     rand.Int31n(TypeMaxID),
	}
}

func (s SensorData) AsJson() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type Server struct {
	sarama.SyncProducer
}

func (s *Server) Start() {
	for {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		msg := &sarama.ProducerMessage{Topic: "telekom-test", Value: sarama.StringEncoder(NewSensorData().AsJson())}
		partition, offset, err := s.SendMessage(msg)
		if err != nil {
			log.Printf("FAILED to send message: %s\n", err)
		} else {
			log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
		}
	}
}

func NewServer() Server {
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	return Server{producer}
}

func main() {
	s := NewServer()
	s.Start()
}
