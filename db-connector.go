package ddd

import (
	"database/sql"
	"log"
	"time"
)

const (
	defaultConnectionAttemps         = 3
	defaultConnectionAttemptInterval = 10
)

var (
	defaultRetryPolicy = RetryPolicy{
		connectionAttempts:        defaultConnectionAttemps,
		connectionAttemptInterval: defaultConnectionAttemptInterval,
	}
)

func Connect(driver string, dns string, dbChannel chan *sql.DB, errorChannel chan error) {
	ConnectWithRetryPolicy(driver, dns, dbChannel, errorChannel, defaultRetryPolicy)
}

func ConnectWithRetryPolicy(driver string, dns string, dbChannel chan *sql.DB, errorChannel chan error, policy RetryPolicy) {
	log.Println("Entering ConnectWithRetryPolicy")
	var db *sql.DB
	var err error
	var attempts int

	for attempts = 0; attempts < policy.connectionAttempts; attempts++ {
		log.Printf("attempt [ %d ] to connect to database \n", attempts)
		db, err = sql.Open(driver, dns)
		if err == nil {
			log.Println("successfully connected to database")
			dbChannel <- db
			break
		}
		log.Println("waiting ", defaultConnectionAttemptInterval, " seconds before next attempt")
		time.Sleep(time.Second * defaultConnectionAttemptInterval)
	}
	if err != nil && db == nil {
		log.Println("cannot retrieve database connection")
		errorChannel <- err
	}
}

type RetryPolicy struct {
	connectionAttempts        int
	connectionAttemptInterval int
}
