/*
Copyright 2016 The Kubernetes Authors All rights reserved.

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

package unversioned

import (
	"fmt"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/watch"
)

type SchedulersInterface interface {
	Schedulers() SchedulerInterface
}

type SchedulerInterface interface {
	Get(name string) (result *api.Scheduler, err error)
	Create(scheduler *api.Scheduler) (*api.Scheduler, error)
	List(opts api.ListOptions) (*api.SchedulerList, error)
	Delete(name string) error
	Update(*api.Scheduler) (*api.Scheduler, error)
	UpdateStatus(*api.Scheduler) (*api.Scheduler, error)
	Watch(opts api.ListOptions) (watch.Interface, error)
}

// schedulers implements SchedulersInterface
type schedulers struct {
	r *Client
}

// newSchedulers returns a schedulers object.
func newSchedulers(c *Client) *schedulers {
	return &schedulers{c}
}

// resourceName returns scheduler's URL resource name.
func (c *schedulers) resourceName() string {
	return "schedulers"
}

// Create creates a new scheduler.
func (c *schedulers) Create(scheduler *api.Scheduler) (*api.Scheduler, error) {
	result := &api.Scheduler{}
	err := c.r.Post().Resource(c.resourceName()).Body(scheduler).Do().Into(result)
	return result, err
}

// List takes a selector, and returns the list of schedulers that match that selector in the cluster.
func (c *schedulers) List(opts api.ListOptions) (*api.SchedulerList, error) {
	result := &api.SchedulerList{}
	err := c.r.Get().Resource(c.resourceName()).VersionedParams(&opts, api.Scheme).Do().Into(result)
	return result, err
}

// Get gets an existing scheduler.
func (c *schedulers) Get(name string) (*api.Scheduler, error) {
	result := &api.Scheduler{}
	err := c.r.Get().Resource(c.resourceName()).Name(name).Do().Into(result)
	return result, err
}

// Delete deletes an existing scheduler.
func (c *schedulers) Delete(name string) error {
	return c.r.Delete().Resource(c.resourceName()).Name(name).Do().Error()
}

// Update updates an existing scheduler.
func (c *schedulers) Update(scheduler *api.Scheduler) (*api.Scheduler, error) {
	result := &api.Scheduler{}
	if len(scheduler.ResourceVersion) == 0 {
		err := fmt.Errorf("invalid update object, missing resource version: %v", scheduler)
		return nil, err
	}
	err := c.r.Put().Resource(c.resourceName()).Name(scheduler.Name).Body(scheduler).Do().Into(result)
	return result, err
}

func (c *schedulers) UpdateStatus(scheduler *api.Scheduler) (*api.Scheduler, error) {
	result := &api.Scheduler{}
	if len(scheduler.ResourceVersion) == 0 {
		err := fmt.Errorf("invalid update object, missing resource version: %v", scheduler)
		return nil, err
	}
	err := c.r.Put().Resource(c.resourceName()).Name(scheduler.Name).SubResource("status").Body(scheduler).Do().Into(result)
	return result, err
}

// Watch returns a watch.Interface that watches the requested schedulers.
func (c *schedulers) Watch(opts api.ListOptions) (watch.Interface, error) {
	return c.r.Get().
		Prefix("watch").
		Namespace(api.NamespaceAll).
		Resource(c.resourceName()).
		VersionedParams(&opts, api.Scheme).
		Watch()
}
