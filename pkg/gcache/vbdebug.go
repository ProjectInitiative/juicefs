package gcache

import (
	"log"
	"os"
)

var verbsDebug = os.Getenv("VB_DEBUG") != ""

func vbDebugf(format string, args ...any) {
	if verbsDebug {
		log.Printf("[vb] "+format, args...)
	}
}
