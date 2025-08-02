package main

import (
	"bytes"
	"context"
	"log"
	"runtime"
	"sync"

	"github.com/minio/minio-go/v7"
)

// Uploader handles batched MinIO uploads
type Uploader struct {
	ingestor *Ingestor
	pool     *WorkerPool[UploadJob]
}

// NewUploader initializes a new Uploader
func NewUploader(ingestor *Ingestor) *Uploader {
	return &Uploader{
		ingestor: ingestor,
		pool: NewWorkerPool[UploadJob]("Uploader", 2*runtime.NumCPU(), ingestor.uploadQueue, func(ctx context.Context, id int, job UploadJob) error {
			bufferReader := bytes.NewReader(job.Buffer.Bytes())
			_, err := ingestor.MinIO.PutObject(ctx, ingestor.Cfg.FeaturesBucketName, job.ObjectName, bufferReader, int64(job.Buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
			if err != nil {
				log.Printf("Uploader %d failed to upload %s: %v", id, job.ObjectName, err)
				return err
			}
			log.Printf("Uploader %d uploaded %s", id, job.ObjectName)
			return nil
		}),
	}
}

// Start launches uploader workers
func (u *Uploader) Start(ctx context.Context, wg *sync.WaitGroup) {
	u.pool.Start(ctx, wg)
}
