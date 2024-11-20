package progressive

/*
func TestFindMostCurrentChildOfUpgradeState(t *testing.T) {
	ctx := context.TODO()
	var rolloutObject ctlrcommon.RolloutObject
	upgradeState := common.UpgradeState("test-state")
	checkLive := false
	c := fake.NewClientBuilder().Build()

	tests := []struct {
		name          string
		children      *unstructured.UnstructuredList
		expectedName  string
		expectedError error
	}{
		{
			name: "Multiple children with valid indices",
			children: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "child-1"}}},
					{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "child-2"}}},
					{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "child-3"}}},
				},
			},
			expectedName:  "child-3",
			expectedError: nil,
		},
		{
			name:          "No children",
			children:      &unstructured.UnstructuredList{},
			expectedName:  "",
			expectedError: nil,
		},
		{
			name: "Improperly named child",
			children: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "child-1"}}},
					{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "improper-name"}}},
					{Object: map[string]interface{}{"metadata": map[string]interface{}{"name": "child-3"}}},
				},
			},
			expectedName:  "child-3",
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock the findChildrenOfUpgradeState function
			findChildrenOfUpgradeState = func(ctx context.Context, rolloutObject ctlrcommon.RolloutObject, upgradeState common.UpgradeState, checkLive bool, c client.Client) (*unstructured.UnstructuredList, error) {
				return tt.children, nil
			}

			mostCurrentChild, err := progressive.FindMostCurrentChildOfUpgradeState(ctx, rolloutObject, upgradeState, checkLive, c)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Nil(t, mostCurrentChild)
			} else {
				assert.NoError(t, err)
				if tt.expectedName == "" {
					assert.Nil(t, mostCurrentChild)
				} else {
					assert.NotNil(t, mostCurrentChild)
					assert.Equal(t, tt.expectedName, mostCurrentChild.GetName())
				}
			}
		})
	}
}
*/
