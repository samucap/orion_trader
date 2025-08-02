package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestWorkerPoolConcurrency(t *testing.T) {
	synctest.Run(func() {
		// Test setup
		numWorkers := 4
		jobCount := 100
		var processedJobs atomic.Int32
		queue := make(chan int, jobCount)

		// Worker function that simulates work
		workFn := func(ctx context.Context, id int, job int) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				processedJobs.Add(1)
				time.Sleep(10 * time.Millisecond) // Simulate work
				return nil
			}
		}

		// Create and start worker pool
		wp := NewWorkerPool[int]("TestPool", numWorkers, queue, workFn)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		go wp.Start(ctx, &wg)

		// Feed jobs
		for i := 0; i < jobCount; i++ {
			queue <- i
		}
		close(queue)

		// Wait for all goroutines to block
		synctest.Wait()

		// Verify all jobs processed
		if processedJobs.Load() != int32(jobCount) {
			t.Errorf("Expected %d jobs processed, got %d", jobCount, processedJobs.Load())
		}
	})
}

func TestWorkerPoolFailureHandling(t *testing.T) {
	synctest.Run(func() {
		// Test setup
		numWorkers := 2
		queue := make(chan int, 10)
		var failureCount atomic.Int32

		// Worker function that fails half the time
		workFn := func(ctx context.Context, id int, job int) error {
			if job%2 == 0 {
				failureCount.Add(1)
				return errors.New("simulated failure")
			}
			return nil
		}

		// Create and start worker pool
		wp := NewWorkerPool[int]("FailurePool", numWorkers, queue, workFn)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		go wp.Start(ctx, &wg)

		// Feed 10 jobs
		for i := 0; i < 10; i++ {
			queue <- i
		}
		close(queue)

		// Wait for all goroutines to block
		synctest.Wait()

		// Verify failures
		if failureCount.Load() != 5 {
			t.Errorf("Expected 5 failures, got %d", failureCount.Load())
		}
	})
}

func TestWorkerPoolContextCancellation(t *testing.T) {
	synctest.Run(func() {
		// Test setup
		numWorkers := 3
		queue := make(chan int, 100)
		var processedJobs atomic.Int32

		// Worker function
		workFn := func(ctx context.Context, id int, job int) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				processedJobs.Add(1)
				time.Sleep(100 * time.Millisecond) // Slow work
				return nil
			}
		}

		// Create and start worker pool
		wp := NewWorkerPool[int]("CancelPool", numWorkers, queue, workFn)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		go wp.Start(ctx, &wg)

		// Feed some jobs
		for i := 0; i < 50; i++ {
			queue <- i
		}

		// Cancel context early
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		cancel()

		// Feed more jobs (should be ignored)
		for i := 50; i < 100; i++ {
			select {
			case queue <- i:
			default:
			}
		}
		close(queue)

		// Wait for workers to finish
		synctest.Wait()

		// Verify partial processing
		if processedJobs.Load() >= 50 {
			t.Errorf("Expected fewer than 50 jobs processed, got %d", processedJobs.Load())
		}
	})
}
