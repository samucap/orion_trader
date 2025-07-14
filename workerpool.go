package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
)

// WorkerPool manages a pool of concurrent workers processing jobs from a queue
type WorkerPool struct {
	name      string
	numWorkers int
	workers   atomic.Int64
	workFn    func(ctx context.Context, id int, job interface{}) error
	queue     interface{} // Generic channel
}

// NewWorkerPool creates a new WorkerPool
func NewWorkerPool(name string, numWorkers int, queue interface{}, workFn func(ctx context.Context, id int, job interface{}) error) *WorkerPool {
	return &WorkerPool{
		name:       name,
		numWorkers: numWorkers,
		workFn:     workFn,
		queue:      queue,
	}
}

// Start launches the worker pool
func (wp *WorkerPool) Start(ctx context.Context, wg *sync.WaitGroup) {
	for i := 0; i < wp.numWorkers; i++ {
		wg.Add(1)
		go wp.worker(ctx, wg, i)
	}
}

func (wp *WorkerPool) worker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	wp.workers.Add(1)
	defer wp.workers.Add(-1)
	log.Printf("%s worker %d started.", wp.name, id)

	switch queue := wp.queue.(type) {
	case chan []string:
		for batch := range queue {
			if err := wp.workFn(ctx, id, batch); err != nil {
				log.Printf("%s worker %d failed: %v", wp.name, id, err)
			}
		}
	case chan FetchedData:
		for data := range queue {
			if err := wp.workFn(ctx, id, data); err != nil {
				log.Printf("%s worker %d failed: %v", wp.name, id, err)
			}
		}
	case chan UploadJob:
		for job := range queue {
			if err := wp.workFn(ctx, id, job); err != nil {
				log.Printf("%s worker %d failed: %v", wp.name, id, err)
			}
		}
	default:
		log.Printf("%s worker %d: unsupported queue type", wp.name, id)
	}
	log.Printf("%s worker %d stopped.", wp.name, id)
}
