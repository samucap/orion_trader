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
	pool     *WorkerPool
}

// NewUploader initializes a new Uploader
func NewUploader(ingestor *Ingestor) *Uploader {
	return &Uploader{
		ingestor: ingestor,
		pool: NewWorkerPool("Uploader", 2*runtime.NumCPU(), ingestor.uploadQueue, func(ctx context.Context, id int, job interface{}) error {
			uploadJob := job.(UploadJob)
			bufferReader := bytes.NewReader(uploadJob.Buffer.Bytes())
			_, err := ingestor.MinIO.PutObject(ctx, ingestor.Cfg.FeaturesBucketName, uploadJob.ObjectName, bufferReader, int64(uploadJob.Buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
			if err != nil {
				log.Printf("Uploader %d failed to upload %s: %v", id, uploadJob.ObjectName, err)
				return err
			}
			log.Printf("Uploader %d uploaded %s", id, uploadJob.ObjectName)
			return nil
		}),
	}
}

// Start launches uploader workers
func (u *Uploader) Start(ctx context.Context, wg *sync.WaitGroup) {
	u.pool.Start(ctx, wg)
}
