package lib

import "log"

func Rows(r int64, err error) int64 {
	if err != nil {
		log.Println("cannot retrieved number of Rows affected : ", err.Error())
		return int64(0)
	}
	return r
}
