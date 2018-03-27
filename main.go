/*
The main.go file is only here for test purpose
*/


package ddd

import (
	"os"
	"fmt"
	"net/http"
	"encoding/json"

	"github.com/gorilla/mux"
	"github.com/satori/go.uuid"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)


var (
	eventPublisherService EventPublisherService
	eventSubscriberService EventSubscriberService
)

//Event sent and received from Kafka, which contains the DomainEvent to save
type GenericEvent struct {
	AggregateEvent 		`json:"aggregateEvent"`
	Data 		string `json:"data"`
}

type AnotherEvent struct {
	SagaEvent 		`json:"sagaEvent"`
	GenericRequest		`json:"genericRequest"`
	Data 		string `json:"data"`
}

type AnAggregateEvent struct {
	AggregateEvent       `json:"aggregateEvent"`
	GenericRequest		`json:"genericRequest"`
	Data 		string `json:"data"`
}

//Generic HTTP request
type GenericRequest struct {
	Title string `json:"title"`
	Data  string `json:"data"`
}

func main() {
	var err error
	brokerUrl := os.Getenv("BROKER_URL")
	datasource := os.Getenv("DATASOURCE")

	//Event store service
	eventStoreService := NewEventStoreService(datasource)
	go eventStoreService.Initialize()

	//Event publisher
	eventPublisherService, err = NewKafkaPublisherService(brokerUrl, eventStoreService)
	if err != nil {
		fmt.Println("cannot build eventPublisherService : ", err.Error())
	}

	//Event subscriber
	eventSubscriberService, err := NewKafkaSubscriberService(brokerUrl, "issuer-adapter-aes-vts", eventStoreService)
	if err != nil {
		fmt.Println("cannot build kafka event subscriber service : ", err.Error())
	}

	//Add an handler for a specific pair of topic/event
	eventSubscriberService.AddHandler("domain.genericEvent", "genericEvent", genericEventHandler)
	eventSubscriberService.AddHandler("domain.genericEvent", "anotherEvent", anotherEventHandler)
	//Subscribe to topics
	go eventSubscriberService.Subscribe()

	//Handle http requests
	r := mux.NewRouter()
	r.HandleFunc("/", rootHandler)
	r.HandleFunc("/sendEvent", sendEventHandler)
	http.ListenAndServe(":8080", r)
}

func rootHandler(w http.ResponseWriter, r *http.Request){
	fmt.Println("Access root")
}

func sendEventHandler(w http.ResponseWriter, r *http.Request){
	fmt.Println("sendEventHandler()")

	//Parse http request
	p := GenericRequest{}
	json.NewDecoder(r.Body).Decode(&p)

	//Create the event
	traceId := uuid.NewV4()
	event := GenericEvent{
		AggregateEvent: NewAggregateEvent("genericType", traceId.String(), "Generic", "123456" ),
		Data: p.Title + " | " + p.Data,
	}

	//Publish it through Kafka
	eventPublisherService.Publish(event)

	event2 := AnotherEvent{
		SagaEvent: NewSagaEvent("AddToken", traceId.String(), "anotherEvent"),
		GenericRequest: GenericRequest{
			Title: p.Title,
			Data:  p.Data,
		},
		Data: "This is the data of another event",
	}

	//Publish it through Kafka
	eventPublisherService.Publish(event2)

	//Http response
	w.Write([]byte(p.Title + " | " + p.Data))
}

//Called when the domain.genericEvent/genericEvent is received
func genericEventHandler(message *kafka.Message) (err error){
	fmt.Println("genericEventHandler()")

	//Parse kafkaMessage
	event := GenericEvent{}
	ParseEvent(message.Value, &event)

	//Process the event
	fmt.Println(event)

	return
}

func anotherEventHandler(message *kafka.Message) (err error){
	fmt.Println("anotherEventHandler()")

	//Parse kafkaMessage
	event := AnotherEvent{}
	ParseEvent(message.Value, &event)

	//Process the event
	fmt.Println(event)

	return
}


