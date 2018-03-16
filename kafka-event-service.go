package ddd

import (
	"fmt"
	"os"
	"errors"
	"encoding/json"
	"reflect"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-kit/kit/log"
	"github.com/Jeffail/gabs"
	"github.com/satori/go.uuid"
)

func NewKafkaPublisherService(brokerUrl string, eventStoreService EventStoreService) (es EventPublisherService, err error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokerUrl})
	if err != nil {
		return
	}
	return &kafkaPublisherService{
		brokerUrl:         brokerUrl,
		Producer:          p,
		EventStoreService: eventStoreService,
		Logger:            log.NewLogfmtLogger(os.Stdout),
	}, nil
}

type kafkaPublisherService struct {
	brokerUrl string
	*kafka.Producer
	EventStoreService
	log.Logger
}

type kafkaSubscriberService struct {
	brokerUrl                string
	*kafka.Consumer
	log.Logger
	EventStoreService
	funcMap					map[string]map[string]func(message *kafka.Message) (error)
	hasHandler				bool
}

//Store the DomainEvent and send the event
func (ks kafkaPublisherService) Publish(event interface{}) (err error) {
	domainEvent, jsonEvent, err := store(ks, event)
	if err != nil {
		ks.Log("error", fmt.Sprintf("cannot store event : %s", err.Error()))
		return
	}
	// publish event in kafka
	ks.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &domainEvent.Aggregate.AggregateType, Partition: kafka.PartitionAny},
		Value:          jsonEvent,
		Key:            uuid.NewV4().Bytes(),
	}
	return
}

func NewKafkaSubscriberService(brokerUrl string, groupId string, eventStoreService EventStoreService) (EventSubscriberService, error) {
	var err error
	ks := kafkaSubscriberService{
		brokerUrl:                brokerUrl,
		Logger:                   log.NewLogfmtLogger(os.Stdout),
		EventStoreService:        eventStoreService,
		funcMap:				  make(map[string]map[string]func(message *kafka.Message) (error)),
	}
	ks.Consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               brokerUrl,
		"group.id":                        groupId,
		"session.timeout.ms":              60000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})
	return &ks, err
}

//Add or replace the given handler for a specific topic/event pair
func (ks *kafkaSubscriberService) AddHandler(topic string, event string, f func(message *kafka.Message) (error)){
	_, prs := ks.funcMap[topic]
	if !prs {
		ks.funcMap[topic] = make(map[string]func(message *kafka.Message) (error))
	}
	ks.funcMap[topic][event] = f
	ks.hasHandler = true
}

//Subscribe to all topics set by AddHandler
func (ks kafkaSubscriberService) Subscribe() (err error){

	if !ks.hasHandler {
		err = errors.New("The list of topic is empty. You must call AddHandler first")
		fmt.Println("Subscribe : ", err)
		return
	}

	//Get the keys of the map
	keys := []string{}
	for key, _ := range ks.funcMap {
		keys = append(keys, key)
	}
	ks.subscribe(keys)

	return
}

// Subcribe to kafka topics
// Call onMessageReceived for each message event read from kafka
func (ks kafkaSubscriberService) subscribe(topics []string) {
	ks.Log("msg", "subscribing to topics")
	ks.Log("topics", topics[0])
	err := ks.SubscribeTopics(topics, nil)
	if err != nil {
		ks.Log("msg", fmt.Sprintf("cannot subsribe to kafka topics : %s", err.Error()))
		return
	}
	for {
		ev := <-ks.Events()
		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			//fmt.Fprintf(os.Stderr, "%% %v\n", e)
			ks.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			//fmt.Fprintf(os.Stderr, "%% %v\n", e)
			ks.Unassign()
		case *kafka.Message:
			err := ks.OnMessageReceivedHandler()(e)
			if err != nil {
				ks.Log("error", err.Error())
			}
		case kafka.PartitionEOF:
			//fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		}
	}
	return
}

// Return the function to invoke when an event message is read from kafka
func (ks kafkaSubscriberService) OnMessageReceivedHandler() (func(message *kafka.Message) (error)) {
	return ks.onKafkaMessageReceived
}

// handle kafka event and invoke right services
func (ks kafkaSubscriberService) onKafkaMessageReceived(message *kafka.Message) (err error) {

	//Retrieve the domainEvent from the received event
	domainEvent, err := ks.toDomainEvent(message.Value)
	if err != nil {
		ks.Log("error", err.Error())
		return
	}

	topic := *message.TopicPartition.Topic
	eventType := domainEvent.EventType

	//Use the map to check if the topic/event pair exists
	if _, topicPrs := ks.funcMap[topic]; topicPrs {
		if _, eventPrs := ks.funcMap[topic][eventType]; eventPrs {

			//Store the received event
			err = ks.EventStoreService.Store(&domainEvent, string(message.Value))
			if err != nil {
				return
			}

			//Call the method
			err = ks.funcMap[topic][eventType](message)
		}
	}

	if err != nil {
		//TODO : Error during call -> update db
		ks.Log("error", err.Error())
	}
	return
}

//Parse the event to retrieve the DomainEvent object
func (ks kafkaSubscriberService) toDomainEvent(value []byte) (DomainEvent, error) {
	var err error

	domainEvent := DomainEvent{}
	jsonParsed, err := gabs.ParseJSON(value)
	if err != nil {
		ks.Log("error", fmt.Sprintf("Cannot parse JSON kafka message : %s", err.Error()))
		return domainEvent, err
	}

	s := jsonParsed.Search("domainEvent").String()
	if s == "{}" {
		err = errors.New("cannot retrieve domainEvent in kafka message")
		ks.Log("error", err.Error())
		return domainEvent, err
	}

	err = json.Unmarshal([]byte(s), &domainEvent)
	if err != nil{
		ks.Log("error", err.Error())
	}
	return domainEvent, err
}

//Store the event from []byte
func (ks kafkaSubscriberService) store(event []byte) (domainEvent DomainEvent, err error) {
	domainEvent, err = ks.toDomainEvent(event)
	if err != nil {
		return
	}
	err = ks.Store(&domainEvent, string(event))
	return
}

//Store the event from interface
func store(eventStoreService EventStoreService, event interface{}) (domainEvent DomainEvent, payload []byte, err error) {
	rf := reflect.ValueOf(event)
	domainEvent = rf.FieldByName("DomainEvent").Interface().(DomainEvent)
	payload = JsonEvent(event)
	// call EventStoreService
	err = eventStoreService.Store(&domainEvent, string(payload))
	return
}



