package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

// WorkerPool manages a pool of concurrent workers processing jobs from a queue
type WorkerPool[T any] struct {
	name       string
	numWorkers int
	workers    atomic.Int64
	workFn     func(ctx context.Context, id int, job T) error
	queue      chan T
}

// NewWorkerPool creates a new WorkerPool
func NewWorkerPool[T any](name string, numWorkers int, queue chan T, workFn func(ctx context.Context, id int, job T) error) *WorkerPool[T] {
	return &WorkerPool[T]{
		name:       name,
		numWorkers: numWorkers,
		workFn:     workFn,
		queue:      queue,
	}
}

// Start launches the worker pool
func (wp *WorkerPool[T]) Start(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < wp.numWorkers; i++ {
		wg.Add(1)
		go wp.worker(ctx, wg, i)
	}
}

func (wp *WorkerPool[T]) worker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	wp.workers.Add(1)
	defer wp.workers.Add(-1)
	log.Printf("%s worker %d started.", wp.name, id)

	for job := range wp.queue {
		if err := wp.workFn(ctx, id, job); err != nil {
			log.Printf("%s worker %d failed: %v", wp.name, id, err)
		}
	}
	log.Printf("%s worker %d stopped.", wp.name, id)
}
