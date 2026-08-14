package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/uday1o1/chronicle-gate/internal/app"
)

func main() {
	context, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-context.Done()
		stop()
	}()

	code := app.Execute(context, os.Args[1:], os.Stdout, os.Stderr, app.Dependencies{})
	os.Exit(int(code))
}
