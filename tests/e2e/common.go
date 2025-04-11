package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/ptr"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	clientgo "k8s.io/client-go/kubernetes"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	planeversiond "github.com/numaproj/numaplane/pkg/client/clientset/versioned"
	planepkg "github.com/numaproj/numaplane/pkg/client/clientset/versioned/typed/numaplane/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	kubeconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	dynamicClient dynamic.DynamicClient
	testEnv       *envtest.Environment
	ctx           context.Context
	// Note: this timeout needs to be large enough for:
	//  - progressive child resource healthiness assessment (2 minutes until assessment start time + 1 minute until end time)
	//  - time for isbsvc to be created plus pipeline to become healthy afterward
	TestTimeout         = 6 * time.Minute
	TestPollingInterval = 10 * time.Millisecond

	pipelineRolloutClient           planepkg.PipelineRolloutInterface
	isbServiceRolloutClient         planepkg.ISBServiceRolloutInterface
	numaflowControllerRolloutClient planepkg.NumaflowControllerRolloutInterface
	numaflowControllerClient        planepkg.NumaflowControllerInterface
	monoVertexRolloutClient         planepkg.MonoVertexRolloutInterface
	kubeClient                      clientgo.Interface

	wg     sync.WaitGroup
	mutex  sync.RWMutex
	stopCh chan struct{}

	disableTestArtifacts string
	enablePodLogs        string

	UpgradeStrategy config.USDEUserStrategy

	openFiles map[string]*os.File
)

const (
	InitialNumaflowControllerVersion = "1.4.3"
	UpdatedNumaflowControllerVersion = "1.4.4-rc1" // note this one is really a clone of "1.4.3"

	Namespace = "numaplane-system"

	ControllerOutputPath = "../output/controllers"

	ResourceChangesPipelineOutputPath           = "../output/resources/pipelinerollouts"
	ResourceChangesISBServiceOutputPath         = "../output/resources/isbservicerollouts"
	ResourceChangesMonoVertexOutputPath         = "../output/resources/monovertexrollouts"
	ResourceChangesNumaflowControllerOutputPath = "../output/resources/numaflowcontrollerrollouts"

	PodLogsPipelineOutputPath            = "../output/logs/pipelinerollouts"
	PodLogsISBServiceOutputPath          = "../output/logs/isbservicerollouts"
	PodLogsMonoVertexOutputPath          = "../output/logs/monovertexrollouts"
	PodLogsNumaflowControllerOutputPath  = "../output/logs/numaflowcontrollerrollouts"
	PodLogsNumaplaneControllerOutputPath = "../output/logs/numaplanecontroller"

	NumaplaneAPIVersion = "numaplane.numaproj.io/v1alpha1"
	NumaflowAPIVersion  = "numaflow.numaproj.io/v1alpha1"

	NumaplaneLabel = "app.kubernetes.io/part-of=numaplane"
	NumaflowLabel  = "app.kubernetes.io/part-of=numaflow"

	ParentRolloutLabel        = "numaplane.numaproj.io/parent-rollout-name"
	UpgradeStateLabelSelector = "numaplane.numaproj.io/upgrade-state=promoted"

	LogSpacer = "================================"
)

type Output struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   metav1.ObjectMeta `json:"metadata"`
	Spec       interface{}       `json:"spec"`
	Status     interface{}       `json:"status,omitempty"`
}

type PipelineRolloutInfo struct {
	CurrentCount        int
	PipelineRolloutName string
	PipelineIsFailed    bool
}

type ComponentType = string

const (
	ComponentVertex     ComponentType = "vertex"
	ComponentMonoVertex ComponentType = "mono-vertex"
)

func verifyPodsRunning(namespace string, numPods int, labelSelector string) {
	CheckEventually(fmt.Sprintf("verifying %d Pods running with label selector %q", numPods, labelSelector), func() bool {
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
	}).WithTimeout(TestTimeout).Should(BeTrue())

}

func VerifyVerticesPodsRunning(namespace, rolloutChildName string, specVertices []numaflowv1.AbstractVertex, component ComponentType) {
	baseLabelSelector := fmt.Sprintf("%s=%s,%s=%s",
		numaflowv1.KeyPartOf, "numaflow",
		numaflowv1.KeyComponent, component,
	)

	msg := ""
	switch component {
	case ComponentVertex:
		baseLabelSelector = fmt.Sprintf("%s,%s=%s", baseLabelSelector, numaflowv1.KeyPipelineName, rolloutChildName)
		msg = "for each Pipeline Vertex"
	case ComponentMonoVertex:
		baseLabelSelector = fmt.Sprintf("%s,%s=%s", baseLabelSelector, numaflowv1.KeyMonoVertexName, rolloutChildName)
		msg = "for the MonoVertex"
	}

	CheckEventually(fmt.Sprintf("verifying that the correct number of Pods is running %s (%s)", msg, rolloutChildName), func() bool {
		for _, vtx := range specVertices {
			vtxLabelSelector := ""
			switch component {
			case ComponentVertex:
				vtxLabelSelector = fmt.Sprintf("app.kubernetes.io/name=%s-%s", rolloutChildName, vtx.Name)
			case ComponentMonoVertex:
				vtxLabelSelector = fmt.Sprintf("app.kubernetes.io/name=%s", rolloutChildName)
			}

			labelSelector := fmt.Sprintf("%s,%s", baseLabelSelector, vtxLabelSelector)

			min := vtx.Scale.Min
			if min == nil {
				min = ptr.To(int32(0))
			}

			max := vtx.Scale.Max
			if max == nil {
				max = ptr.To(int32(numaflowv1.DefaultMaxReplicas))
			}

			podsList, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
			if err != nil {
				return false
			}

			if podsList == nil {
				return false
			}

			vtxPodsCount := int32(len(podsList.Items))
			if vtxPodsCount < *min || vtxPodsCount > *max {
				return false
			}

			for _, pod := range podsList.Items {
				if pod.Status.Phase != "Running" {
					return false
				}
			}
		}

		return true
	}).WithTimeout(TestTimeout).Should(BeTrue())
}

func getRolloutCondition(conditions []metav1.Condition, conditionType apiv1.ConditionType) *metav1.Condition {
	for _, cond := range conditions {
		if cond.Type == string(conditionType) {
			return &cond
		}
	}
	return nil
}

func getRolloutConditionStatus(conditions []metav1.Condition, conditionType apiv1.ConditionType) metav1.ConditionStatus {
	c := getRolloutCondition(conditions, conditionType)
	if c == nil {
		return metav1.ConditionUnknown
	}
	return c.Status
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
			if event.Type == watch.Added || event.Type == watch.Modified {
				pod := event.Object.(*corev1.Pod)
				for _, container := range pod.Spec.Containers {
					streamPodLogs(context.Background(), kubeClient, Namespace, pod.Name, container.Name, stopCh)
				}
				for _, container := range pod.Spec.InitContainers {
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

			return false, nil
		})

		if err != nil {
			// TODO: log this as an error using a logger library
			fmt.Printf("Failed to stream pod %q logs: %v", podName, err)
			return
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

	watchResourceType(func() (watch.Interface, error) {
		watcher, err := kubeClient.CoreV1().Pods(Namespace).Watch(context.Background(), metav1.ListOptions{LabelSelector: NumaflowLabel})
		return watcher, err
	}, func(o runtime.Object) Output {
		if pod, ok := o.(*corev1.Pod); ok {
			pod.ManagedFields = nil
			return Output{
				APIVersion: "v1",
				Kind:       "Pod",
				Metadata:   pod.ObjectMeta,
				Spec:       pod.Spec,
				Status:     pod.Status,
			}
		}
		return Output{}
	})

}

func watchResourceType(getWatchFunc func() (watch.Interface, error), processEventObject func(runtime.Object) Output) {

	defer wg.Done()
	watcher, err := getWatchFunc()
	if err != nil {
		fmt.Printf("Failed to start watcher: %v\n", err)
		return
	}
	defer watcher.Stop()

	for {
		select {
		case event := <-watcher.ResultChan():
			if event.Type == watch.Modified {
				output := processEventObject(event.Object)
				err = writeToFile(output)
				if err != nil {
					return
				}
			}
		case <-stopCh:
			return
		}
	}

}

// helper func to write `kubectl get -o yaml` output to file
func writeToFile(resource Output) error {

	mutex.Lock()
	defer mutex.Unlock()

	var fileName string

	switch resource.Kind {
	case "Pipeline":
		fileName = filepath.Join(ResourceChangesPipelineOutputPath, "pipeline.yaml")
	case "Vertex":
		fileName = filepath.Join(ResourceChangesPipelineOutputPath, "vertices", strings.Join([]string{resource.Metadata.Name, ".yaml"}, ""))
	case "PipelineRollout":
		fileName = filepath.Join(ResourceChangesPipelineOutputPath, "pipeline_rollout.yaml")
	case "InterStepBufferService":
		fileName = filepath.Join(ResourceChangesISBServiceOutputPath, "isbservice.yaml")
	case "StatefulSet":
		fileName = filepath.Join(ResourceChangesISBServiceOutputPath, "statefulsets", strings.Join([]string{resource.Metadata.Name, ".yaml"}, ""))
	case "ISBServiceRollout":
		fileName = filepath.Join(ResourceChangesISBServiceOutputPath, "isbservice_rollout.yaml")
	case "MonoVertex":
		fileName = filepath.Join(ResourceChangesMonoVertexOutputPath, "monovertex.yaml")
	case "MonoVertexRollout":
		fileName = filepath.Join(ResourceChangesMonoVertexOutputPath, "monovertex_rollout.yaml")
	case "NumaflowController":
		fileName = filepath.Join(ResourceChangesNumaflowControllerOutputPath, "numaflowcontroller.yaml")
	case "NumaflowControllerRollout":
		fileName = filepath.Join(ResourceChangesNumaflowControllerOutputPath, "numaflowcontroller_rollout.yaml")
	case "Pod":
		switch resource.Metadata.Labels["app.kubernetes.io/component"] {
		case "controller-manager":
			fileName = filepath.Join(ResourceChangesNumaflowControllerOutputPath, "pods", strings.Join([]string{resource.Metadata.Name, ".yaml"}, ""))
		case "isbsvc":
			fileName = filepath.Join(ResourceChangesISBServiceOutputPath, "pods", strings.Join([]string{resource.Metadata.Name, ".yaml"}, ""))
		case "mono-vertex", "mono-vertex-daemon":
			fileName = filepath.Join(ResourceChangesMonoVertexOutputPath, "pods", strings.Join([]string{resource.Metadata.Name, ".yaml"}, ""))
		case "daemon", "vertex", "job":
			fileName = filepath.Join(ResourceChangesPipelineOutputPath, "pods", strings.Join([]string{resource.Metadata.Name, ".yaml"}, ""))
		default:
			return nil
		}
	}

	if _, ok := openFiles[fileName]; !ok {
		file, err := os.Create(fileName)
		if err != nil {
			fmt.Printf("Failed to open log file: %v\n", err)
			return err
		}
		openFiles[fileName] = file
	}

	file := openFiles[fileName]
	bytes, _ := yaml.Marshal(resource)
	updateLog := fmt.Sprintf("%s\n%v\n\n%s\n", LogSpacer, time.Now().Format(time.RFC3339Nano), string(bytes))
	_, err := file.WriteString(updateLog)
	if err != nil {
		fmt.Printf("Failed to write to log file: %v\n", err)
		return err
	}

	return nil
}

func closeAllFiles() error {
	for _, file := range openFiles {
		err := file.Sync()
		if err != nil { // flush file to disk
			return err
		}
		err = file.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func getChildResource(gvr schema.GroupVersionResource, namespace, rolloutName string) (*unstructured.Unstructured, error) {

	label := fmt.Sprintf("%s,%s=%s", UpgradeStateLabelSelector, ParentRolloutLabel, rolloutName)

	unstructList, err := dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: label})
	if err != nil {
		return nil, err
	}
	if len(unstructList.Items) == 0 {
		return nil, fmt.Errorf("list is empty")
	}

	return &unstructList.Items[0], nil

}

func GetChildren(gvr schema.GroupVersionResource, namespace, rolloutName string) (*unstructured.UnstructuredList, error) {
	label := fmt.Sprintf("%s=%s", ParentRolloutLabel, rolloutName)

	return dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: label})
}

func GetChildrenOfUpgradeStrategy(gvr schema.GroupVersionResource, namespace, rolloutName string, upgradeState common.UpgradeState) (*unstructured.UnstructuredList, error) {
	label := fmt.Sprintf("%s=%s,%s=%s", ParentRolloutLabel, rolloutName, common.LabelKeyUpgradeState, upgradeState)

	return dynamicClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: label})
}

func GetNumberOfChildren(gvr schema.GroupVersionResource, namespace, rolloutName string) int {
	children, err := GetChildren(gvr, namespace, rolloutName)
	if err != nil || children == nil {
		return 0
	}
	return len(children.Items)
}

func getUpgradeStrategy() config.USDEUserStrategy {
	userStrategy := config.USDEUserStrategy(strings.ToLower(os.Getenv("STRATEGY")))
	if userStrategy == "" {
		return config.NoStrategyID
	} else {
		return userStrategy
	}
}

func BeforeSuiteSetup() {
	var err error
	// make output directory to store temporary outputs; if it's there from before delete it
	disableTestArtifacts = os.Getenv("DISABLE_TEST_ARTIFACTS")
	// pod logs env
	enablePodLogs = os.Getenv("ENABLE_POD_LOGS")
	if disableTestArtifacts != "true" {
		setupOutputDir()
	}
	// this must be set for all tests to run
	UpgradeStrategy = getUpgradeStrategy()
	Expect(UpgradeStrategy.IsValid()).To(BeTrue())

	openFiles = make(map[string]*os.File)

	stopCh = make(chan struct{})
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	ctx = context.Background()

	scheme := runtime.NewScheme()
	err = apiv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())

	err = numaflowv1.AddToScheme(scheme)
	Expect(err).NotTo(HaveOccurred())
	useExistingCluster := true

	restConfig := kubeconfig.GetConfigOrDie()

	testEnv = &envtest.Environment{
		UseExistingCluster:       &useExistingCluster,
		Config:                   restConfig,
		AttachControlPlaneOutput: true,
	}

	cfg, err := testEnv.Start()
	Expect(cfg).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	pipelineRolloutClient = planeversiond.NewForConfigOrDie(cfg).NumaplaneV1alpha1().PipelineRollouts(Namespace)
	Expect(pipelineRolloutClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	monoVertexRolloutClient = planeversiond.NewForConfigOrDie(cfg).NumaplaneV1alpha1().MonoVertexRollouts(Namespace)
	Expect(monoVertexRolloutClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	isbServiceRolloutClient = planeversiond.NewForConfigOrDie(cfg).NumaplaneV1alpha1().ISBServiceRollouts(Namespace)
	Expect(isbServiceRolloutClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	numaflowControllerRolloutClient = planeversiond.NewForConfigOrDie(cfg).NumaplaneV1alpha1().NumaflowControllerRollouts(Namespace)
	Expect(numaflowControllerRolloutClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	numaflowControllerClient = planeversiond.NewForConfigOrDie(cfg).NumaplaneV1alpha1().NumaflowControllers(Namespace)
	Expect(numaflowControllerClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	kubeClient, err = clientgo.NewForConfig(cfg)
	Expect(kubeClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	dynamicClient = *dynamic.NewForConfigOrDie(cfg)
	Expect(dynamicClient).NotTo(BeNil())
	Expect(err).NotTo(HaveOccurred())

	if disableTestArtifacts != "true" {

		wg.Add(1)
		go watchPods()

		startNumaflowControllerRolloutWatches()

		startPipelineRolloutWatches()

		startISBServiceRolloutWatches()

		startMonoVertexRolloutWatches()

		if enablePodLogs == "true" {
			wg.Add(1)
			go watchPodLogs(kubeClient, Namespace, NumaplaneLabel)

			wg.Add(1)
			go watchPodLogs(kubeClient, Namespace, NumaflowLabel)
		}

	}
}

var _ = AfterSuite(func() {

	By("tearing down test environment")
	close(stopCh)

	err := closeAllFiles()
	Expect(err).NotTo(HaveOccurred())

	err = testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())

})

var _ = AfterEach(func() {

	report := CurrentSpecReport()
	if report.Failed() {
		AbortSuite("Test spec has failed, aborting suite run")
	}

})

func setupOutputDir() {

	var (
		dirs = []string{ResourceChangesPipelineOutputPath, ResourceChangesISBServiceOutputPath,
			ResourceChangesMonoVertexOutputPath, ResourceChangesNumaflowControllerOutputPath}
		logDirs = []string{PodLogsPipelineOutputPath, PodLogsISBServiceOutputPath,
			PodLogsNumaflowControllerOutputPath, PodLogsMonoVertexOutputPath, PodLogsNumaplaneControllerOutputPath}
	)

	// clear out prior runs output files
	directory := "../output"
	_, err := os.Stat(directory)
	if err == nil {
		err = os.RemoveAll(directory)
		Expect(err).NotTo(HaveOccurred())
	}

	// output/resources contains `kubectl get` output for each resource
	if disableTestArtifacts != "true" {
		for _, dir := range dirs {
			if dir == ResourceChangesPipelineOutputPath {
				err = os.MkdirAll(filepath.Join(dir, "vertices"), os.ModePerm)
				Expect(err).NotTo(HaveOccurred())
			}
			if dir == ResourceChangesISBServiceOutputPath {
				err = os.MkdirAll(filepath.Join(dir, "statefulsets"), os.ModePerm)
				Expect(err).NotTo(HaveOccurred())
			}
			err = os.MkdirAll(filepath.Join(dir, "pods"), os.ModePerm)
			Expect(err).NotTo(HaveOccurred())
		}

		// output/pods contains pod logs for each resource
		if enablePodLogs == "true" {
			for _, dir := range logDirs {
				err = os.MkdirAll(dir, os.ModePerm)
				Expect(err).NotTo(HaveOccurred())
			}
		}
	}

}

// CheckEventually is wrappers around Ginkgo's Eventually
// You can override the default timeout and polling interval by using WithTimeout and WithPolling methods
func CheckEventually(testData string, actualOrCtx interface{}) AsyncAssertion {
	By(testData)
	return Eventually(actualOrCtx, TestTimeout, TestPollingInterval)
}

// CheckConsistently is wrappers around Ginkgo's Consistently
// You can override the default timeout and polling interval by using WithTimeout and WithPolling methods
func CheckConsistently(testData string, actualOrCtx interface{}) AsyncAssertion {
	By(testData)
	return Consistently(actualOrCtx, TestTimeout, TestPollingInterval)
}

func VerifyVerticesScale(actualVertexScaleMap map[string]numaflowv1.Scale, expectedVertexScaleMap map[string]numaflowv1.Scale) bool {
	for expectedVertexName, expectedVertexScale := range expectedVertexScaleMap {
		actualVertexScale, exists := actualVertexScaleMap[expectedVertexName]
		if !exists {
			return false
		}

		if !reflect.DeepEqual(actualVertexScale, expectedVertexScale) {
			return false
		}
	}

	return true
}
