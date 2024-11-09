package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func Test_inProgressStrategyMgr_getStrategy(t *testing.T) {

	progressiveStrategy := apiv1.UpgradeStrategyProgressive
	ppndStrategy := apiv1.UpgradeStrategyPPND
	noStrategy := apiv1.UpgradeStrategyNoOp

	testCases := []struct {
		name                  string
		inMemoryStrategy      *apiv1.UpgradeStrategy
		rolloutStatusStrategy *apiv1.UpgradeStrategy
		resultStrategy        apiv1.UpgradeStrategy
	}{
		{
			name:                  "in memory and in Rollout Status (progressive result)",
			inMemoryStrategy:      &progressiveStrategy,
			rolloutStatusStrategy: &noStrategy,
			resultStrategy:        progressiveStrategy,
		},
		{
			name:                  "in memory and in Rollout Status (no op result)",
			inMemoryStrategy:      &noStrategy,
			rolloutStatusStrategy: &ppndStrategy,
			resultStrategy:        noStrategy,
		},
		{
			name:                  "in memory and not in Rollout Status",
			inMemoryStrategy:      &progressiveStrategy,
			rolloutStatusStrategy: nil,
			resultStrategy:        progressiveStrategy,
		},
		{
			name:                  "in Rollout Status and not in memory",
			inMemoryStrategy:      nil,
			rolloutStatusStrategy: &ppndStrategy,
			resultStrategy:        ppndStrategy,
		},
		{
			name:                  "neither in Rollout Status nor in memory",
			inMemoryStrategy:      nil,
			rolloutStatusStrategy: nil,
			resultStrategy:        noStrategy,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inProgressStrategyMgr := newInProgressStrategyMgr(
				// getRolloutStrategy function:
				func(ctx context.Context, rollout client.Object) *apiv1.UpgradeStrategy {
					return tc.rolloutStatusStrategy
				},
				// setRolloutStrategy function:
				func(ctx context.Context, rollout client.Object, strategy apiv1.UpgradeStrategy) {},
			)
			pipelineRollout := &apiv1.PipelineRollout{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "my-pipeline"}}
			namespacedName := k8stypes.NamespacedName{Namespace: pipelineRollout.GetNamespace(), Name: pipelineRollout.GetName()}
			if tc.inMemoryStrategy != nil {
				inProgressStrategyMgr.store.setStrategy(namespacedName, *tc.inMemoryStrategy)
			}
			upgradeStrategyResult := inProgressStrategyMgr.getStrategy(context.Background(), pipelineRollout)
			assert.Equal(t, tc.resultStrategy, upgradeStrategyResult)
		})
	}
}
