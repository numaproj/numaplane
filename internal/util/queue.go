package util

import (
	"k8s.io/client-go/util/workqueue"
)

// rateLimitingQueue is a wrapper around RateLimitingInterface, which is essentially a queue that offers the following:
// - if used correctly, only worker receives a given key at a time
// - keys are rate limited to create fairness between keys
// - keys can be scheduled to be re-processed at a certain time
// TODO: Note that this wrapper doesn't provide any added benefit beyond the RateLimitingInterface that it wraps, but we can add
// metrics for queue length to this by imitating this: https://github.com/argoproj/argo-workflows/blob/main/workflow/metrics/work_queue.go
type rateLimitingQueue struct {
	workqueue.TypedRateLimitingInterface[interface{}]
	workerType string
}

func NewWorkQueue(queueName string) workqueue.TypedRateLimitingInterface[interface{}] {
	return rateLimitingQueue{
		TypedRateLimitingInterface: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[interface{}]()),
		workerType:                 queueName,
	}
}

func (w rateLimitingQueue) Get() (interface{}, bool) {
	item, shutdown := w.TypedRateLimitingInterface.Get()
	return item, shutdown
}

func (w rateLimitingQueue) Done(item interface{}) {
	w.TypedRateLimitingInterface.Done(item)
}
