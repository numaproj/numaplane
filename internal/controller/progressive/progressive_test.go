package progressive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	numaflowv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaplane/internal/common"
	ctlrcommon "github.com/numaproj/numaplane/internal/controller/common"
	"github.com/numaproj/numaplane/internal/controller/config"
	"github.com/numaproj/numaplane/internal/util/kubernetes"
	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	commontest "github.com/numaproj/numaplane/tests/common"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type fakeProgressiveController struct{}

func (fpc fakeProgressiveController) CreateUpgradingChildDefinition(ctx context.Context, rolloutObject ProgressiveRolloutObject, name string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func (fpc fakeProgressiveController) IncrementChildCount(ctx context.Context, rolloutObject ctlrcommon.RolloutObject) (int32, error) {
	return 0, nil
}

func (fpc fakeProgressiveController) Recycle(ctx context.Context, childObject *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ChildNeedsUpdating(ctx context.Context, existingChild, newChildDefinition *unstructured.Unstructured) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) AssessUpgradingChild(ctx context.Context, existingUpgradingChildDef *unstructured.Unstructured) (apiv1.AssessmentResult, error) {
	switch existingUpgradingChildDef.GetName() {
	case "test-success":
		return apiv1.AssessmentResultSuccess, nil
	case "test-failure":
		return apiv1.AssessmentResultFailure, nil
	default:
		return apiv1.AssessmentResultUnknown, nil
	}
}

func (fpc fakeProgressiveController) ProcessPromotedChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessPromotedChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, promotedChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessUpgradingChildPostFailure(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func (fpc fakeProgressiveController) ProcessUpgradingChildPostSuccess(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) error {
	return nil
}

func (fpc fakeProgressiveController) ProcessUpgradingChildPreUpgrade(ctx context.Context, rolloutObject ProgressiveRolloutObject, upgradingChildDef *unstructured.Unstructured, c client.Client) (bool, error) {
	return false, nil
}

func Test_processUpgradingChild(t *testing.T) {
	restConfig, numaflowClientSet, client, _, err := commontest.PrepareK8SEnvironment()
	assert.Nil(t, err)
	assert.Nil(t, kubernetes.SetClientSets(restConfig))

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig2"))
	assert.NoError(t, err)

	ctx := context.Background()

	globalConfig, err := config.GetConfigManagerInstance().GetConfig()
	assert.NoError(t, err)

	assessmentSchedule, err := globalConfig.Progressive.GetChildStatusAssessmentSchedule("MonoVertex")
	assert.NoError(t, err)

	defaultExistingPromotedChildDef := createMonoVertex("test")

	testCases := []struct {
		name                      string
		rolloutObject             ProgressiveRolloutObject
		existingUpgradingChildDef *numaflowv1.MonoVertex
		expectedDone              bool
		expectedRequeueDelay      time.Duration
		expectedError             error
	}{
		{
			name:                      "no upgrading child status on the live rollout",
			rolloutObject:             defaultMonoVertexRollout.DeepCopy(),
			existingUpgradingChildDef: createMonoVertex("test"),
			expectedDone:              false,
			expectedRequeueDelay:      assessmentSchedule.Interval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - different name",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{Name: "test"}},
				},
				nil),
			//setRolloutObjectChildStatus(defaultMonoVertexRollout.DeepCopy(), &apiv1.UpgradingChildStatus{Name: "test"}, &apiv1.PromotedChildStatus{}),
			existingUpgradingChildDef: createMonoVertex("test-1"),
			expectedDone:              false,
			expectedRequeueDelay:      assessmentSchedule.Interval,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - same name, can assess, success",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                "test-success",
							AssessmentResult:    apiv1.AssessmentResultUnknown,
							AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
							AssessmentEndTime:   &metav1.Time{Time: time.Now()},
						},
					},
				},
				nil,
			),
			existingUpgradingChildDef: createMonoVertex("test-success"),
			expectedDone:              true,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
		{
			name: "preset upgrading child status on the live rollout - same name, failure",
			rolloutObject: setMonoVertexProgressiveStatus(
				defaultMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                "test-failure",
							AssessmentResult:    apiv1.AssessmentResultFailure,
							AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						},
					},
				},
				&apiv1.PromotedMonoVertexStatus{
					PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
						PromotedChildStatus: apiv1.PromotedChildStatus{
							Name: defaultExistingPromotedChildDef.GetName(),
						},
						ScaleValuesRestoredToOriginal: true,
					},
				},
			),
			existingUpgradingChildDef: createMonoVertex("test-failure"),
			expectedDone:              false,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
		{
			name: "force promote a failed progressive upgrade",
			rolloutObject: setMonoVertexProgressiveStatus(
				forcePromoteMonoVertexRollout.DeepCopy(),
				&apiv1.UpgradingMonoVertexStatus{
					UpgradingPipelineTypeStatus: apiv1.UpgradingPipelineTypeStatus{
						UpgradingChildStatus: apiv1.UpgradingChildStatus{
							Name:                "test-force-promote",
							AssessmentResult:    apiv1.AssessmentResultFailure,
							AssessmentStartTime: &metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						},
					},
				},
				&apiv1.PromotedMonoVertexStatus{
					PromotedPipelineTypeStatus: apiv1.PromotedPipelineTypeStatus{
						PromotedChildStatus: apiv1.PromotedChildStatus{
							Name: defaultExistingPromotedChildDef.GetName(),
						},
						ScaleValuesRestoredToOriginal: true,
					},
				},
			),
			existingUpgradingChildDef: createMonoVertex("test-force-promote"),
			expectedDone:              true,
			expectedRequeueDelay:      0,
			expectedError:             nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			// first delete MonoVertex and MonoVertexRollout in case they already exist, in Kubernetes
			_ = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})

			monoVertexList, err := numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Len(t, monoVertexList.Items, 0)

			// creating existingPromotedChild and existingUpgradingChild MonoVertices, in Kubernetes
			_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, tc.existingUpgradingChildDef, metav1.CreateOptions{})
			assert.NoError(t, err)

			if tc.existingUpgradingChildDef.Name != defaultExistingPromotedChildDef.Name {
				_, err = numaflowClientSet.NumaflowV1alpha1().MonoVertices(ctlrcommon.DefaultTestNamespace).Create(ctx, defaultExistingPromotedChildDef, metav1.CreateOptions{})
				assert.NoError(t, err)
			}

			actualDone, actualRequeueDelay, actualErr := processUpgradingChild(
				ctx, tc.rolloutObject, fakeProgressiveController{}, monoVertexToUnstruct(defaultExistingPromotedChildDef), monoVertexToUnstruct(tc.existingUpgradingChildDef), client)

			if tc.expectedError != nil {
				assert.Error(t, actualErr)
				assert.False(t, actualDone)
				assert.Zero(t, actualRequeueDelay)
			} else {
				assert.Nil(t, actualErr)
				assert.Equal(t, tc.expectedDone, actualDone)
				assert.Equal(t, tc.expectedRequeueDelay, actualRequeueDelay)
			}
		})
	}
}

func setMonoVertexProgressiveStatus(mvRollout *apiv1.MonoVertexRollout, upgradingChildStatus *apiv1.UpgradingMonoVertexStatus, promotedChildStatus *apiv1.PromotedMonoVertexStatus) *apiv1.MonoVertexRollout {
	mvRollout.Status.ProgressiveStatus.UpgradingMonoVertexStatus = upgradingChildStatus
	mvRollout.Status.ProgressiveStatus.PromotedMonoVertexStatus = promotedChildStatus
	return mvRollout
}

var (
	monoVertexSpec = numaflowv1.MonoVertexSpec{
		Replicas: ptr.To(int32(1)),
		Source: &numaflowv1.Source{
			UDSource: &numaflowv1.UDSource{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-java/source-simple-source:stable",
				},
			},
			UDTransformer: &numaflowv1.UDTransformer{
				Container: &numaflowv1.Container{
					Image: "quay.io/numaio/numaflow-rs/source-transformer-now:stable",
				},
			},
		},
		Sink: &numaflowv1.Sink{
			AbstractSink: numaflowv1.AbstractSink{
				UDSink: &numaflowv1.UDSink{
					Container: &numaflowv1.Container{
						Image: "quay.io/numaio/numaflow-java/simple-sink:stable",
					},
				},
			},
		},
	}
)

func createMonoVertex(name string) *numaflowv1.MonoVertex {

	return ctlrcommon.CreateTestMonoVertexOfSpec(
		monoVertexSpec, name,
		numaflowv1.MonoVertexPhaseRunning,
		numaflowv1.Status{
			Conditions: []metav1.Condition{
				{
					Type:               string(numaflowv1.MonoVertexConditionDaemonHealthy),
					Status:             metav1.ConditionTrue,
					Reason:             "healthy",
					LastTransitionTime: metav1.NewTime(time.Now()),
				},
			},
		},
		map[string]string{
			common.LabelKeyParentRollout: ctlrcommon.DefaultTestMonoVertexRolloutName,
		},
		map[string]string{
			common.AnnotationKeyNumaflowInstanceID: "1",
		})

}

func monoVertexToUnstruct(mvtx *numaflowv1.MonoVertex) *unstructured.Unstructured {
	unstructMap, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(mvtx)
	return &unstructured.Unstructured{Object: unstructMap}
}

var defaultMonoVertexRollout = &apiv1.MonoVertexRollout{
	ObjectMeta: metav1.ObjectMeta{
		Name: "test",
	},
	Status: apiv1.MonoVertexRolloutStatus{
		ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
			UpgradingMonoVertexStatus: nil,
			PromotedMonoVertexStatus:  nil,
		},
	},
}

var forcePromoteMonoVertexRollout = &apiv1.MonoVertexRollout{
	ObjectMeta: metav1.ObjectMeta{
		Name: "test",
	},
	Status: apiv1.MonoVertexRolloutStatus{
		ProgressiveStatus: apiv1.MonoVertexProgressiveStatus{
			UpgradingMonoVertexStatus: nil,
			PromotedMonoVertexStatus:  nil,
		},
	},
	Spec: apiv1.MonoVertexRolloutSpec{
		Strategy: &apiv1.PipelineTypeRolloutStrategy{
			PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
				Progressive: apiv1.ProgressiveStrategy{
					ForcePromote: true,
				},
			},
		},
	},
}

func Test_CalculateScaleMinMaxValues(t *testing.T) {
	testCases := []struct {
		name           string
		object         map[string]any
		podsCount      int
		pathToMin      []string
		expectedNewMin int64
		expectedNewMax int64
		expectedError  error
	}{
		{
			name:           "min less than newMax",
			object:         map[string]any{"scale": map[string]any{"min": int64(2)}},
			podsCount:      10,
			pathToMin:      []string{"scale", "min"},
			expectedNewMin: 2,
			expectedNewMax: 5,
			expectedError:  nil,
		},
		{
			name:           "zero pods",
			object:         map[string]any{"scale": map[string]any{"min": int64(1)}},
			podsCount:      0,
			pathToMin:      []string{"scale", "min"},
			expectedNewMin: 0,
			expectedNewMax: 0,
			expectedError:  nil,
		},
		{
			name:           "min equals newMax",
			object:         map[string]any{"scale": map[string]any{"min": int64(5)}},
			podsCount:      10,
			pathToMin:      []string{"scale", "min"},
			expectedNewMin: 5,
			expectedNewMax: 5,
			expectedError:  nil,
		},
		{
			name:           "min not set",
			object:         map[string]any{"scale": map[string]any{}},
			podsCount:      10,
			pathToMin:      []string{"scale", "min"},
			expectedNewMin: 0,
			expectedNewMax: 5,
			expectedError:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualNewMin, actualNewMax, actualErr := CalculateScaleMinMaxValues(tc.object, tc.podsCount, tc.pathToMin)

			if tc.expectedError != nil {
				assert.Error(t, actualErr)
				assert.Equal(t, -1, actualNewMin)
				assert.Equal(t, -1, actualNewMax)
			} else {
				assert.Nil(t, actualErr)
				assert.Equal(t, tc.expectedNewMin, actualNewMin)
				assert.Equal(t, tc.expectedNewMax, actualNewMax)
			}
		})
	}
}

func Test_getChildStatusAssessmentSchedule(t *testing.T) {

	getwd, err := os.Getwd()
	assert.Nil(t, err, "Failed to get working directory")
	configPath := filepath.Join(getwd, "../../../", "tests", "config")
	configManager := config.GetConfigManagerInstance()
	err = configManager.LoadAllConfigs(func(err error) {}, config.WithConfigsPath(configPath), config.WithConfigFileName("testconfig"))
	assert.NoError(t, err)

	testCases := []struct {
		name             string
		rolloutSchedule  string
		expectedSchedule config.AssessmentSchedule
		expectedError    bool
	}{
		{
			name:            "rollout defines schedule",
			rolloutSchedule: "300,200,10",
			expectedSchedule: config.AssessmentSchedule{
				Delay:    300 * time.Second,
				Period:   200 * time.Second,
				Interval: 10 * time.Second,
			},
			expectedError: false,
		},
		{
			name:            "rollout doesn't define schedule, use default for the Kind",
			rolloutSchedule: "",
			expectedSchedule: config.AssessmentSchedule{
				Delay:    120 * time.Second,
				Period:   60 * time.Second,
				Interval: 10 * time.Second,
			},
			expectedError: false,
		},
		{
			name:            "rollout defines invalid format, so default is used for the Kind",
			rolloutSchedule: "10,20",
			expectedSchedule: config.AssessmentSchedule{
				Delay:    120 * time.Second,
				Period:   60 * time.Second,
				Interval: 10 * time.Second,
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			pipelineRollout := apiv1.PipelineRollout{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-pipeline",
					Namespace: "my-namespace",
				},
				Spec: apiv1.PipelineRolloutSpec{
					Pipeline: apiv1.Pipeline{
						// not needed for test
					},
					Strategy: &apiv1.PipelineTypeRolloutStrategy{
						PipelineTypeProgressiveStrategy: apiv1.PipelineTypeProgressiveStrategy{
							Progressive: apiv1.ProgressiveStrategy{
								AssessmentSchedule: tc.rolloutSchedule,
							},
						},
					},
				},
			}

			resultSchedule, err := getChildStatusAssessmentSchedule(context.Background(), &pipelineRollout)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedSchedule, resultSchedule)
			}
		})
	}
}

func Test_ExtractOriginalScaleMinMaxAsJSONString(t *testing.T) {
	testCases := []struct {
		name               string
		object             map[string]any
		pathToScale        []string
		expectedJSONString string
		expectedError      error
	}{
		{
			name:               "scale unset",
			object:             map[string]any{"spec": map[string]any{"somefield": int64(2)}},
			pathToScale:        []string{"spec", "scale"},
			expectedJSONString: "null",
			expectedError:      nil,
		},
		{
			name:               "min set",
			object:             map[string]any{"scale": map[string]any{"min": int64(1)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"max\":null,\"min\":1}",
			expectedError:      nil,
		},
		{
			name:               "max set",
			object:             map[string]any{"scale": map[string]any{"max": int64(5)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"max\":5,\"min\":null}",
			expectedError:      nil,
		},
		{
			name:               "both min and max set",
			object:             map[string]any{"scale": map[string]any{"min": int64(3), "max": int64(7)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"max\":7,\"min\":3}",
			expectedError:      nil,
		},
		{
			name:               "both min and max set and also another field",
			object:             map[string]any{"scale": map[string]any{"min": int64(3), "max": int64(7), "zeroReplicaSleepSeconds": int64(123)}},
			pathToScale:        []string{"scale"},
			expectedJSONString: "{\"max\":7,\"min\":3}",
			expectedError:      nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualJSONString, actualErr := ExtractOriginalScaleMinMaxAsJSONString(tc.object, tc.pathToScale)

			if tc.expectedError != nil {
				assert.Error(t, actualErr)
				assert.Equal(t, "", actualJSONString)
			} else {
				assert.Nil(t, actualErr)
				assert.Equal(t, tc.expectedJSONString, actualJSONString)
			}
		})
	}
}

func Test_AreVertexReplicasReady(t *testing.T) {
	testCases := []struct {
		name            string
		desiredReplicas *int64
		readyReplicas   *int64
		expectedResult  bool
	}{
		{
			name:            "desired = 0, ready = 0",
			desiredReplicas: ptr.To(int64(0)),
			readyReplicas:   ptr.To(int64(0)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready = 0",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   ptr.To(int64(0)),
			expectedResult:  false,
		},
		{
			name:            "desired = 0, ready > 0",
			desiredReplicas: ptr.To(int64(0)),
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready > 0, desired < ready",
			desiredReplicas: ptr.To(int64(2)),
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready > 0, desired > ready",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   ptr.To(int64(2)),
			expectedResult:  false,
		},
		{
			name:            "desired > 0, ready > 0, desired = ready",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired = nil, ready = nil",
			desiredReplicas: nil,
			readyReplicas:   nil,
			expectedResult:  true,
		},
		{
			name:            "desired = nil, ready = 0",
			desiredReplicas: nil,
			readyReplicas:   ptr.To(int64(0)),
			expectedResult:  true,
		},
		{
			name:            "desired = 0, ready = nil",
			desiredReplicas: ptr.To(int64(0)),
			readyReplicas:   nil,
			expectedResult:  true,
		},
		{
			name:            "desired = nil, ready > 0",
			desiredReplicas: nil,
			readyReplicas:   ptr.To(int64(3)),
			expectedResult:  true,
		},
		{
			name:            "desired > 0, ready = nil",
			desiredReplicas: ptr.To(int64(3)),
			readyReplicas:   nil,
			expectedResult:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			unstr := &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{},
				},
			}

			if tc.desiredReplicas != nil {
				_ = unstructured.SetNestedField(unstr.Object, *tc.desiredReplicas, "status", "desiredReplicas")
			}

			if tc.readyReplicas != nil {
				_ = unstructured.SetNestedField(unstr.Object, *tc.readyReplicas, "status", "readyReplicas")
			}

			actualResult, actualErr := AreVertexReplicasReady(unstr)

			assert.Nil(t, actualErr)
			assert.Equal(t, tc.expectedResult, actualResult)
		})
	}
}
