package e2e

import (
	"context"
	"fmt"
	"io"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	apiv1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	clientgo "k8s.io/client-go/kubernetes"

	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	planepkg "github.com/numaproj/numaplane/pkg/client/clientset/versioned/typed/numaplane/v1alpha1"
)

var (
	dynamicClient dynamic.DynamicClient
	testEnv       *envtest.Environment
	ctx           context.Context
	cancel        context.CancelFunc
	suiteTimeout  = 5 * time.Minute
	testTimeout   = 2 * time.Minute

	pipelineRolloutClient           planepkg.PipelineRolloutInterface
	isbServiceRolloutClient         planepkg.ISBServiceRolloutInterface
	numaflowControllerRolloutClient planepkg.NumaflowControllerRolloutInterface
	kubeClient                      clientgo.Interface
)

const (
	Namespace = "numaplane-system"
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
		fmt.Println("***** Pods not in Running state:")
		// print more details about Pods not in Running state:
		for _, pod := range podList.Items {
			if pod.Status.Phase != v1.PodRunning {
				fmt.Printf("Pod: %q, %q\n", pod.Name, pod.Status.Phase)
				fmt.Printf(" Conditions:\n   %+v\n", pod.Status.Conditions)
				// if there's an unitialized container, print its logs
				unitializedContainer := false
				for _, condition := range pod.Status.Conditions {
					if condition.Reason == "ContainersNotInitialized" {
						unitializedContainer = true
						break
					}
				}
				if unitializedContainer {
					for _, initContainer := range pod.Spec.InitContainers {
						printPodLogs(kubeClient, Namespace, pod.Name, initContainer.Name)
					}
				}

			}

		}
	}
}

func verifyPodsRunning(namespace string, numPods int, labelSelector string) {
	document(fmt.Sprintf("verifying %d Pods running with label selector %q", numPods, labelSelector))

	Eventually(func() bool {
		podsList, _ := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if podsList != nil && len(podsList.Items) >= numPods {
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

func getNumaflowResourceStatus(u *unstructured.Unstructured) (kubernetes.GenericStatus, error) {
	statusMap := u.Object["status"]
	var status kubernetes.GenericStatus
	err := util.StructToStruct(&statusMap, &status)
	return status, err
}

func printPodLogs(client clientgo.Interface, namespace, podName, containerName string) {
	podLogOptions := &apiv1.PodLogOptions{Container: containerName}
	stream, err := client.CoreV1().Pods(namespace).GetLogs(podName, podLogOptions).Stream(ctx)
	if err != nil {
		fmt.Printf("Error getting Pod logs: namespace=%q, pod=%q, container=%q\n", namespace, podName, containerName)
		return
	}
	defer stream.Close()
	logBytes, _ := io.ReadAll(stream)
	fmt.Printf("Printing Log for namespace=%q, pod=%q, container=%q:\n%s\n", namespace, podName, containerName, string(logBytes))
}