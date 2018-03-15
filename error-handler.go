package ddd

import "github.com/gin-gonic/gin/json"

type DomainError struct {
	TraceId string `json:"traceId"`
	Error   string  `json:"error"`
	Payload string `json:"payload"`
}

func NewDomainError(traceId string, err error, v interface{}) DomainError {
	return DomainError{
		TraceId: traceId,
		Error:   err.Error(),
		Payload: mustJson(v),
	}
}

func (d DomainError) String() string {
	return mustJson(d)
}

func mustJson(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "nil"
	}
	return string(b)
}
