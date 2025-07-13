package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutdown signal received...")
		cancel()
	}()

	ingestor, err := NewIngestor()
	if err != nil {
		log.Fatalf("FATAL: Could not initialize ingestor: %v", err)
	}

	if err := ingestor.Run(ctx); err != nil {
		log.Fatalf("FATAL: Ingestor failed: %v", err)
	}
}
