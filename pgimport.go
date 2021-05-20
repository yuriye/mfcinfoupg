package mfcinfoupg

import (
	"time"
)

const ClarionToUnixDiff int64 = 61730

//Стандартная дата Кларион - это число дней, прошедших с 28 декабря 1800 года.
//const CLARION_BASE string = "1800-12-28"
//const UNIX_BASE string = "1970-01-01"

func ClarT2UnixT(clarionDate int64) time.Time {
	sec := (clarionDate - ClarionToUnixDiff) * int64(86400)
	return time.Unix(sec, 0)
}
