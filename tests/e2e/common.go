package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	clientgo "k8s.io/client-go/kubernetes"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	planepkg "github.com/numaproj/numaplane/pkg/client/clientset/versioned/typed/numaplane/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

var (
	dynamicClient       dynamic.DynamicClient
	testEnv             *envtest.Environment
	ctx                 context.Context
	cancel              context.CancelFunc
	suiteTimeout        = 30 * time.Minute // Note: if we start seeing "client rate limiter: context deadline exceeded", we need to increase this value
	testTimeout         = 2 * time.Minute
	testPollingInterval = 10 * time.Millisecond

	pipelineRolloutClient           planepkg.PipelineRolloutInterface
	isbServiceRolloutClient         planepkg.ISBServiceRolloutInterface
	numaflowControllerRolloutClient planepkg.NumaflowControllerRolloutInterface
	monoVertexRolloutClient         planepkg.MonoVertexRolloutInterface
	kubeClient                      clientgo.Interface

	wg     sync.WaitGroup
	mutex  sync.RWMutex
	stopCh chan struct{}

	ppnd                 string
	disableTestArtifacts string
	enablePodLogs        string

	openFiles map[string]*os.File
)

const (
	Namespace = "numaplane-system"

	ControllerOutputPath = "output/controllers"

	ResourceChangesPipelineOutputPath           = "output/resources/pipelinerollouts"
	ResourceChangesISBServiceOutputPath         = "output/resources/isbservicerollouts"
	ResourceChangesMonoVertexOutputPath         = "output/resources/monovertexrollouts"
	ResourceChangesNumaflowControllerOutputPath = "output/resources/numaflowcontrollerrollouts"

	PodLogsPipelineOutputPath            = "output/logs/pipelinerollouts"
	PodLogsISBServiceOutputPath          = "output/logs/isbservicerollouts"
	PodLogsMonoVertexOutputPath          = "output/logs/monovertexrollouts"
	PodLogsNumaflowControllerOutputPath  = "output/logs/numaflowcontrollerrollouts"
	PodLogsNumaplaneControllerOutputPath = "output/logs/numaplanecontroller"

	NumaplaneAPIVersion = "numaplane.numaproj.io/v1alpha1"
	NumaflowAPIVersion  = "numaflow.numaproj.io/v1alpha1"

	NumaplaneLabel = "app.kubernetes.io/part-of=numaplane"
	NumaflowLabel  = "app.kubernetes.io/part-of=numaflow"

	LogSpacer = "================================"
)

type Output struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   metav1.ObjectMeta `json:"metadata"`
	Spec       interface{}       `json:"spec"`
	Status     interface{}       `json:"status,omitempty"`
}

// document for Ginkgo framework and print to console
func document(testName string) {
	snapshotCluster(testName)
	By(testName)
}

func snapshotCluster(testName string) {
	fmt.Printf("*** %+v: NAMESPACE POD STATE BEFORE TEST: %s\n", time.Now(), testName)
	podList, _ := kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{})
	if podList != nil {
		for _, pod := range podList.Items {
			fmt.Printf("Pod: %q, %q\n", pod.Name, pod.Status.Phase)
		}
	}
}

func verifyPodsRunning(namespace string, numPods int, labelSelector string) {
	document(fmt.Sprintf("verifying %d Pods running with label selector %q", numPods, labelSelector))

	Eventually(func() bool {
		podsList, _ := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if podsList != nil && len(podsList.Items) == numPods {
			for _, pod := range podsList.Items {
				if pod.Status.Phase != "Running" {
					return false
				}
			}
			return true
		}
		return false

	}).WithTimeout(testTimeout).Should(BeTrue())

}

func getRolloutCondition(conditions []metav1.Condition, conditionType apiv1.ConditionType) metav1.ConditionStatus {
	for _, cond := range conditions {
		if cond.Type == string(conditionType) {
			return cond.Status
		}
	}
	return metav1.ConditionUnknown
}

func getNumaflowResourceStatus(u *unstructured.Unstructured) (kubernetes.GenericStatus, error) {
	statusMap := u.Object["status"]
	var status kubernetes.GenericStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func watchPodLogs(client clientgo.Interface, namespace, labelSelector string) {
	watcher, err := client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
	if err != nil {
		fmt.Printf("Error listing pods: %v\n", err)
		return
	}

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Added {
				pod := event.Object.(*corev1.Pod)
				for _, container := range pod.Spec.Containers {
					streamPodLogs(context.Background(), kubeClient, Namespace, pod.Name, container.Name, stopCh)
				}
			}
		case <-stopCh:
			return
		}
	}

}

func streamPodLogs(ctx context.Context, client clientgo.Interface, namespace, podName, containerName string, stopCh <-chan struct{}) {
	var retryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0,
		Steps:    10,
		Duration: time.Second * 1,
	}

	go func() {
		var stream io.ReadCloser
		err := wait.ExponentialBackoffWithContext(ctx, retryBackOff, func(_ context.Context) (done bool, err error) {
			stream, err = client.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Follow: true, Container: containerName}).Stream(ctx)
			if err == nil {
				return true, nil
			}

			fmt.Printf("Got error %v, retrying.\n", err)
			return false, nil
		})

		if err != nil {
			log.Fatalf("Failed to stream pod %q logs: %v", podName, err)
		}
		defer func() { _ = stream.Close() }()

		s := bufio.NewScanner(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			default:
				if !s.Scan() {
					if s.Err() != nil {
						fmt.Printf("Error streaming pod %q logs: %v", podName, s.Err())
					}
					return
				}
				data := s.Bytes()

				var fileName string
				if strings.Contains(podName, "pipeline") {
					fileName = fmt.Sprintf("%s/%s-%s.log", PodLogsPipelineOutputPath, podName, containerName)
				} else if strings.Contains(podName, "isbsvc") {
					fileName = fmt.Sprintf("%s/%s-%s.log", PodLogsISBServiceOutputPath, podName, containerName)
				} else if strings.Contains(podName, "numaflow") {
					fileName = fmt.Sprintf("%s/%s-%s.log", PodLogsNumaflowControllerOutputPath, podName, containerName)
				} else if strings.Contains(podName, "monovertex") {
					fileName = fmt.Sprintf("%s/%s-%s.log", PodLogsMonoVertexOutputPath, podName, containerName)
				} else if strings.Contains(podName, "numaplane") {
					fileName = fmt.Sprintf("%s/%s-%s.log", PodLogsNumaplaneControllerOutputPath, podName, containerName)
				}

				file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Printf("Failed to open log file: %v\n", err)
					return
				}
				defer file.Close()

				updateLog := fmt.Sprintf("%s\n", string(data))
				_, err = file.WriteString(updateLog)
				if err != nil {
					fmt.Printf("Failed to write to log file: %v\n", err)
					return
				}
			}
		}
	}()
}

func watchPods() {

	ctx := context.Background()
	defer wg.Done()
	watcher, err := kubeClient.CoreV1().Pods(Namespace).Watch(ctx, metav1.ListOptions{LabelSelector: NumaflowLabel})
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				if pod, ok := event.Object.(*corev1.Pod); ok {
					pod.ManagedFields = nil
					pd := Output{
						APIVersion: "v1",
						Kind:       "Pod",
						Metadata:   pod.ObjectMeta,
						Spec:       pod.Spec,
						Status:     pod.Status,
					}

					var fileName string
					switch pod.Labels["app.kubernetes.io/component"] {
					case "controller-manager":
						fileName = filepath.Join(ResourceChangesNumaflowControllerOutputPath, "pods", strings.Join([]string{pod.Name, ".yaml"}, ""))
					case "isbsvc":
						fileName = filepath.Join(ResourceChangesISBServiceOutputPath, "pods", strings.Join([]string{pod.Name, ".yaml"}, ""))
					case "mono-vertex", "mono-vertex-daemon":
						fileName = filepath.Join(ResourceChangesMonoVertexOutputPath, "pods", strings.Join([]string{pod.Name, ".yaml"}, ""))
					case "daemon", "vertex", "job":
						fileName = filepath.Join(ResourceChangesPipelineOutputPath, "pods", strings.Join([]string{pod.Name, ".yaml"}, ""))
					default:
						continue
					}

					err := writeToFile(fileName, pd)
					if err != nil {
						return
					}
				}
			}
		case <-stopCh:
			return
		}
	}
}

// helper func to write `kubectl get -o yaml` output to file
func writeToFile(fileName string, resource Output) error {

	mutex.Lock()
	defer mutex.Unlock()

	file, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return err
	}
	openFiles[fileName] = file

	bytes, _ := yaml.Marshal(resource)
	updateLog := fmt.Sprintf("%s\n%v\n\n%s\n", LogSpacer, time.Now().Format(time.RFC3339Nano), string(bytes))
	_, err = file.WriteString(updateLog)
	if err != nil {
		fmt.Printf("Failed to write to log file: %v\n", err)
		return err
	}

	return nil
}

func closeAllFiles() error {
	for _, file := range openFiles {
		err := file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
