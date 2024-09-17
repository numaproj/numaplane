package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	testPollingInterval = 1 * time.Second

	pipelineRolloutClient           planepkg.PipelineRolloutInterface
	isbServiceRolloutClient         planepkg.ISBServiceRolloutInterface
	numaflowControllerRolloutClient planepkg.NumaflowControllerRolloutInterface
	monoVertexRolloutClient         planepkg.MonoVertexRolloutInterface
	kubeClient                      clientgo.Interface

	dataLossPrevention string
)

const (
	Namespace = "numaplane-system"

	NumaplaneCtrlLogs = "output/numaplane-controller.log"
	NumaflowCtrlLogs  = "output/numaflow-controller.log"

	NumaplaneLabel = "app.kubernetes.io/part-of=numaplane"
	NumaflowLabel  = "app.kubernetes.io/part-of=numaflow, app.kubernetes.io/component=controller-manager"
)

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

func getPodLogs(client clientgo.Interface, namespace, labelSelector, containerName, fileName string) {

	ctx := context.Background()
	podLogOptions := &corev1.PodLogOptions{Container: containerName}

	podList, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		fmt.Printf("Error listing pods: %v\n", err)
		return
	}

	for _, pod := range podList.Items {
		stream, err := client.CoreV1().Pods(namespace).GetLogs(pod.Name, podLogOptions).Stream(ctx)
		if err != nil {
			fmt.Printf("Error getting pods logs: %v\n", err)
			return
		}
		defer stream.Close()
		logBytes, _ := io.ReadAll(stream)

		err = os.WriteFile(fileName, logBytes, 0644)
		if err != nil {
			fmt.Printf("Error writing pod logs to file: %v\n", err)
			return
		}
	}

}
