package lib

import (
	"encoding/json"
)

func JsonEvent(v interface{}) ([]byte) {
	b, _ := json.MarshalIndent(v, "", "\t")
	return b
}
