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
// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1alpha1 "github.com/numaproj/numaplane/pkg/apis/numaplane/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeNumaflowControllers implements NumaflowControllerInterface
type FakeNumaflowControllers struct {
	Fake *FakeNumaplaneV1alpha1
	ns   string
}

var numaflowcontrollersResource = v1alpha1.SchemeGroupVersion.WithResource("numaflowcontrollers")

var numaflowcontrollersKind = v1alpha1.SchemeGroupVersion.WithKind("NumaflowController")

// Get takes name of the numaflowController, and returns the corresponding numaflowController object, and an error if there is any.
func (c *FakeNumaflowControllers) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.NumaflowController, err error) {
	emptyResult := &v1alpha1.NumaflowController{}
	obj, err := c.Fake.
		Invokes(testing.NewGetActionWithOptions(numaflowcontrollersResource, c.ns, name, options), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.NumaflowController), err
}

// List takes label and field selectors, and returns the list of NumaflowControllers that match those selectors.
func (c *FakeNumaflowControllers) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.NumaflowControllerList, err error) {
	emptyResult := &v1alpha1.NumaflowControllerList{}
	obj, err := c.Fake.
		Invokes(testing.NewListActionWithOptions(numaflowcontrollersResource, numaflowcontrollersKind, c.ns, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.NumaflowControllerList{ListMeta: obj.(*v1alpha1.NumaflowControllerList).ListMeta}
	for _, item := range obj.(*v1alpha1.NumaflowControllerList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested numaflowControllers.
func (c *FakeNumaflowControllers) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchActionWithOptions(numaflowcontrollersResource, c.ns, opts))

}

// Create takes the representation of a numaflowController and creates it.  Returns the server's representation of the numaflowController, and an error, if there is any.
func (c *FakeNumaflowControllers) Create(ctx context.Context, numaflowController *v1alpha1.NumaflowController, opts v1.CreateOptions) (result *v1alpha1.NumaflowController, err error) {
	emptyResult := &v1alpha1.NumaflowController{}
	obj, err := c.Fake.
		Invokes(testing.NewCreateActionWithOptions(numaflowcontrollersResource, c.ns, numaflowController, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.NumaflowController), err
}

// Update takes the representation of a numaflowController and updates it. Returns the server's representation of the numaflowController, and an error, if there is any.
func (c *FakeNumaflowControllers) Update(ctx context.Context, numaflowController *v1alpha1.NumaflowController, opts v1.UpdateOptions) (result *v1alpha1.NumaflowController, err error) {
	emptyResult := &v1alpha1.NumaflowController{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateActionWithOptions(numaflowcontrollersResource, c.ns, numaflowController, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.NumaflowController), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeNumaflowControllers) UpdateStatus(ctx context.Context, numaflowController *v1alpha1.NumaflowController, opts v1.UpdateOptions) (result *v1alpha1.NumaflowController, err error) {
	emptyResult := &v1alpha1.NumaflowController{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceActionWithOptions(numaflowcontrollersResource, "status", c.ns, numaflowController, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.NumaflowController), err
}

// Delete takes name of the numaflowController and deletes it. Returns an error if one occurs.
func (c *FakeNumaflowControllers) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(numaflowcontrollersResource, c.ns, name, opts), &v1alpha1.NumaflowController{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeNumaflowControllers) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionActionWithOptions(numaflowcontrollersResource, c.ns, opts, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha1.NumaflowControllerList{})
	return err
}

// Patch applies the patch and returns the patched numaflowController.
func (c *FakeNumaflowControllers) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.NumaflowController, err error) {
	emptyResult := &v1alpha1.NumaflowController{}
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceActionWithOptions(numaflowcontrollersResource, c.ns, name, pt, data, opts, subresources...), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.NumaflowController), err
}
