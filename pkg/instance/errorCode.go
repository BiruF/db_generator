package instance

import (
	"math/rand"
	"sync"
)

var count = 0
var mx sync.Mutex

var codes = []string{
	"ATR000",
	"ATR100",
	"ATR101",
	"ATR102",
	"ATR103",
	"ATR104",
	"ATR105",
	"ATR902",
	"ATR200",
	"ATR201",
	"ATR903",
	"ATR300",
	"ATR301",
	"ATR904",
	"ATR400",
	"ATR401",
	"ATR905",
	"ATR906",
	"ATR410",
	"ATR411",
	"ATR910",
	"ATR420",
	"ATR421",
	"ATR920",
	"ATR430",
	"ATR431",
	"ATR930",
	"ATR440",
	"ATR441",
	"ATR940",
	"ATR450",
	"ATR451",
	"ATR950",
	"ATR907",
	"ATR908",
}

var prefixLen = 7
var withPrefixLen = len(codes) - prefixLen

func getErrorCode() string {
	mx.Lock()
	defer mx.Unlock()

	count++

	if count <= 100 {
		return codes[0]
	} else if count <= 1_000 {
		return codes[1]
	} else if count <= 5_000 {
		return codes[2]
	} else if count <= 10_000 {
		return codes[3]
	} else if count <= 20_000 {
		return codes[4]
	} else if count <= 50_000 {
		return codes[5]
	} else if count <= 100_000 {
		return codes[6]
	} else {
		return codes[prefixLen+rand.Intn(withPrefixLen)]
	}
}
