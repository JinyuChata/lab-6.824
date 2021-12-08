package mr

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func PrintLog(msg string) {
	fmt.Println(time.Now().Format("2006-01-02 15:04:05") +
		" [" + strconv.Itoa(os.Getpid()) + "] " + msg)
}
