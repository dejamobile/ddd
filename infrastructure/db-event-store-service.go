package infrastructure

import (
	"../domain"
	"../lib"

	"log"
	"database/sql"
	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/satori/go.uuid"
)

func NewEventStoreService(dns string) domain.EventStoreService {
	return &eventStoreService{dns: dns}
}

type eventStoreService struct {
	dns string
	*sql.DB
}

func (es *eventStoreService) Initialize() {
	log.Println("Handling database connection asynschronously")
	dbChannel := make(chan *sql.DB, 1)
	errorChannel := make(chan error, 1)
	go lib.Connect("mysql", es.dns, dbChannel, errorChannel)
	select {
	case db := <-dbChannel:
		es.DB = db
		log.Println("EventStoreService database connection completed")
	case err := <-errorChannel:
		log.Println("EventStoreService database connection error : ", err.Error())
	}
}

func (es eventStoreService) Store(event *domain.DomainEvent, payload string) (err error) {
	log.Printf("Entering  eventStoreService.Store [ %s ]\n", event.EventType)
	// check if nil pointer passed
	if event == nil {
		err = errors.New("cannot save nil event")
		return
	}
	// prepare insert statement
	stmt, err := es.Prepare(saveEvent)
	if err != nil {
		return
	}
	// close statement before exiting function
	defer stmt.Close()
	// execute statement insert if not exists
	eventUuid := uuid.Must(uuid.FromString(event.Uuid))
	res, err := stmt.Exec(
		eventUuid.Bytes(),
		"RECORDED",
		event.EventType,
		event.Aggregate.AggregateType,
		uuid.Must(uuid.FromString(event.Aggregate.Id)).Bytes(),
		payload,
		uuid.Must(uuid.FromString(event.TraceId)).Bytes(),
		event.When,
		eventUuid.Bytes(),
	)
	if err != nil {
		return
	}
	log.Printf("query : completed with success, Rows affected : %d", lib.Rows(res.RowsAffected()))
	return
}

const (
	findEventById = "SELECT uuid, status, event_type, aggregate_type, aggregate_id, payload, trace_id, event_when FROM event WHERE uuid = ?"
	saveEvent     = "INSERT INTO event(uuid, status, event_type, aggregate_type, aggregate_id, payload, trace_id, event_when) SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS (SELECT uuid from event WHERE uuid = ?)"
)
