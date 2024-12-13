/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package numaflowcontroller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
)

func Test_resolveManifestTemplate(t *testing.T) {
	defaultInstanceID := "123"

	defaultController := &apiv1.NumaflowController{
		Spec: apiv1.NumaflowControllerSpec{
			InstanceID: defaultInstanceID,
		},
	}

	testCases := []struct {
		name             string
		manifest         string
		controller       *apiv1.NumaflowController
		expectedManifest string
		expectedError    error
	}{
		{
			name:             "nil controller",
			manifest:         "",
			controller:       nil,
			expectedManifest: "",
			expectedError:    nil,
		}, {
			name:             "empty manifest",
			manifest:         "",
			controller:       defaultController,
			expectedManifest: "",
			expectedError:    nil,
		}, {
			name:             "manifest with invalid template field",
			manifest:         "this is {{.Invalid}} invalid",
			controller:       defaultController,
			expectedManifest: "",
			expectedError:    fmt.Errorf("unable to apply information to manifest: template: manifest:1:10: executing \"manifest\" at <.Invalid>: can't evaluate field Invalid in type struct { InstanceSuffix string; InstanceID string }"),
		}, {
			name:     "manifest with valid template and controller without instanceID",
			manifest: "valid-template-no-id{{.InstanceSuffix}}",
			controller: &apiv1.NumaflowController{
				Spec: apiv1.NumaflowControllerSpec{},
			},
			expectedManifest: "valid-template-no-id",
			expectedError:    nil,
		}, {
			name:             "manifest with valid template and controller with instanceID",
			manifest:         "valid-template-no-id{{.InstanceSuffix}}",
			controller:       defaultController,
			expectedManifest: fmt.Sprintf("valid-template-no-id-%s", defaultInstanceID),
			expectedError:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manifestBytes, err := resolveManifestTemplate(tc.manifest, tc.controller)

			if tc.expectedError != nil {
				assert.Error(t, err)
				assert.EqualError(t, err, tc.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expectedManifest, string(manifestBytes))
		})
	}
}
