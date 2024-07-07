package util

import (
	"k8s.io/client-go/util/workqueue"
)

type rateLimitingQueue struct {
	workqueue.RateLimitingInterface
	workerType string

	//metrics    *Metrics
}

func NewWorkQueue(queueName string) workqueue.RateLimitingInterface {
	//m.newWorker(queueName)
	return rateLimitingQueue{
		RateLimitingInterface: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), queueName),
		workerType:            queueName,
		//metrics:               m,
	}
}

/*
func (m *Metrics) newWorker(workerType string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.workersBusy[workerType] = getWorkersBusy(workerType)
}

func (m *Metrics) workerBusy(workerType string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.workersBusy[workerType].Inc()
}

func (m *Metrics) workerFree(workerType string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.workersBusy[workerType].Dec()
}*/

func (w rateLimitingQueue) Get() (interface{}, bool) {
	item, shutdown := w.RateLimitingInterface.Get()
	//w.metrics.workerBusy(w.workerType)
	return item, shutdown
}

func (w rateLimitingQueue) Done(item interface{}) {
	w.RateLimitingInterface.Done(item)
	//w.metrics.workerFree(w.workerType)
}

/*
// fixedIntervalRateLimiter implements the RateLimiter interface in client-go
type fixedIntervalRateLimiter struct{}

// When decides the miminum amount of time between two entries
func (r *fixedIntervalRateLimiter) When(interface{}) time.Duration {
	return 5 * time.Second // can consider making this configurable
}

// Forget indicates that an item is finished being retried.  Doesn't matter whether it's for failing
// or for success, we'll stop tracking it
func (r *fixedIntervalRateLimiter) Forget(interface{}) {}

// NumRequeues returns back how many failures the item has had
func (r *fixedIntervalRateLimiter) NumRequeues(interface{}) int {
	return 1
}
*/
