package ddd

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type EventPublisherService interface {
	Publish(event interface{}, topic string) error
}

type EventSubscriberService interface {
	OnMessageReceivedHandler() (func(message *kafka.Message) (error))
	AddHandler(topic string, event string, f func(message *kafka.Message) (error))
	Subscribe() error
}

type EventStoreService interface{
	Initialize()
	Store(event *Event, topic string, payload string) error
}