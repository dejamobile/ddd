package domain

import (
	"time"
	"encoding/json"

	"github.com/satori/go.uuid"
	"fmt"
)

type Aggregate struct {
	AggregateType string `json:"type"`
	Id            string `json:"id"`
}

func NewAggregate(aggregateType string, aggregateId string) Aggregate {
	return Aggregate{
		AggregateType: aggregateType,
		Id:            aggregateId,
	}
}

type DomainEvent struct {
	Uuid      string    `json:"uuid"`
	Aggregate Aggregate `json:"aggregate"`
	EventType string    `json:"eventType"`
	When      string    `json:"when"`
	TraceId   string    `json:"traceId"`
}

func NewDomainEvent(traceId string, eventType string, aggregate Aggregate) DomainEvent {
	return DomainEvent{
		Uuid:      uuid.NewV4().String(),
		When:      time.Now().Format(time.RFC3339),
		EventType: eventType,
		TraceId:   traceId,
		Aggregate: aggregate,
	}
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
