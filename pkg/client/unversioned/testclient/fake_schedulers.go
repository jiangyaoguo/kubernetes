/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

package testclient

import (
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/watch"
)

// FakeNodes implements NodeInterface. Meant to be embedded into a struct to get a default
// implementation. This makes faking out just the method you want to test easier.
type FakeSchedulers struct {
	Fake *Fake
}

func (c *FakeSchedulers) Get(name string) (*api.Scheduler, error) {
	return nil, nil
}

func (c *FakeSchedulers) List(opts api.ListOptions) (*api.SchedulerList, error) {
	obj, err := c.Fake.Invokes(NewRootListAction("schedulers", opts), &api.SchedulerList{})
	if obj == nil {
		return nil, err
	}

	return obj.(*api.SchedulerList), err
}

func (c *FakeSchedulers) Create(scheduler *api.Scheduler) (*api.Scheduler, error) {
	obj, err := c.Fake.Invokes(NewRootCreateAction("schedulers", scheduler), scheduler)
	if obj == nil {
		return nil, err
	}

	return obj.(*api.Scheduler), err
}

func (c *FakeSchedulers) Update(scheduler *api.Scheduler) (*api.Scheduler, error) {
	obj, err := c.Fake.Invokes(NewRootUpdateAction("schedulers", scheduler), scheduler)
	if obj == nil {
		return nil, err
	}

	return obj.(*api.Scheduler), err
}

func (c *FakeSchedulers) Delete(name string) error {
	_, err := c.Fake.Invokes(NewRootDeleteAction("schedulers", name), &api.Scheduler{})
	return err
}

func (c *FakeSchedulers) Watch(opts api.ListOptions) (watch.Interface, error) {
	return c.Fake.InvokesWatch(NewRootWatchAction("schedulers", opts))
}

func (c *FakeSchedulers) UpdateStatus(scheduler *api.Scheduler) (*api.Scheduler, error) {
	return nil, nil
}
