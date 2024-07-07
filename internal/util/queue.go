package util

import (
	"k8s.io/client-go/util/workqueue"
)

// todo: consider adding metrics to this by imitating this: https://github.com/argoproj/argo-workflows/blob/main/workflow/metrics/work_queue.go

type rateLimitingQueue struct {
	workqueue.RateLimitingInterface
	workerType string
}

func NewWorkQueue(queueName string) workqueue.RateLimitingInterface {
	return rateLimitingQueue{
		RateLimitingInterface: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), queueName),
		workerType:            queueName,
	}
}

func (w rateLimitingQueue) Get() (interface{}, bool) {
	item, shutdown := w.RateLimitingInterface.Get()
	return item, shutdown
}

func (w rateLimitingQueue) Done(item interface{}) {
	w.RateLimitingInterface.Done(item)
}
