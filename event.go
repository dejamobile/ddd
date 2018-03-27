package ddd

import (
	"time"
	"encoding/json"

	"github.com/satori/go.uuid"
	"fmt"
)

const (
	sagaTopicPrefix         = "domain.saga."
	aggregateTopicPrefix = "domain.aggregate."
)


func NewEvent(traceId string, eventType string) Event {
	return Event{
		Uuid:      uuid.NewV4().String(),
		When:      time.Now().Format(time.RFC3339),
		EventType: eventType,
		TraceId:   traceId,
	}
}

func NewAggregateEvent(eventType string, traceId string, aggregateType string, aggregateId string) AggregateEvent {
	return AggregateEvent{
		Event: NewEvent(traceId, eventType),
		AggregateType: aggregateType,
		AggregateId:   aggregateId,
	}
}

func NewSagaEvent(sagaType string, sagaId string, eventType string) SagaEvent {
	return SagaEvent{
		Event: NewEvent(sagaId, eventType),
		SagaId:   sagaId,
		SagaType: sagaType,
	}
}

type Event struct {
	Uuid      string    	`json:"uuid"`
	EventType string    	`json:"eventType"`
	When      string    	`json:"when"`
	TraceId   string    	`json:"traceId"`

}

type TracingEvent struct {
	Event						 	`json:"event"`
	Header					string 	`json:"header"`
	Body			        string  `json:"body"`
}

type FCMNotificationEvent struct {
	Event							`json:"event"`
	EventType				string 	`json:"eventType"`
	Content			        string  `json:"content"`
}

type AggregateEvent struct {
	Event						   `json:"event"`
	AggregateType 			string `json:"aggregateType"`
	AggregateId            			string `json:"aggregateId"`
}

type SagaEvent struct {
	Event						   `json:"event"`
	SagaType 			string 		`json:"sagaType"`
	SagaId            			string `json:"sagaId"`
}


func JsonEvent(v interface{}) ([]byte) {
	b, _ := json.MarshalIndent(v, "", "\t")
	return b
}

func ParseEvent(message []byte, cmd interface{}) (err error){
	err = json.Unmarshal(message, &cmd)
	if err != nil {
		fmt.Println("error", fmt.Sprintf("cannot decode event : %s", err.Error()))
	}
	return
}
