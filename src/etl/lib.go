package etl

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func HandleQuitSignal() {
	//signal handling, for elegant quit
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGQUIT)
	s := <-ch
	log.Println("etl get signal:", s)
}
