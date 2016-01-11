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

package scheduler

import (
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/rest"
	"k8s.io/kubernetes/pkg/watch"
)

// Registry is an interface for things that know how to store node.
type Registry interface {
	ListSchedulers(ctx api.Context, options *api.ListOptions) (*api.SchedulerList, error)
	CreateScheduler(ctx api.Context, node *api.Scheduler) error
	UpdateScheduler(ctx api.Context, node *api.Scheduler) error
	GetScheduler(ctx api.Context, schedulerID string) (*api.Scheduler, error)
	DeleteScheduler(ctx api.Context, schedulerID string) error
	WatchSchedulers(ctx api.Context, options *api.ListOptions) (watch.Interface, error)
}

// storage puts strong typing around storage calls
type storage struct {
	rest.StandardStorage
}

// NewRegistry returns a new Registry interface for the given Storage. Any mismatched
// types will panic.
func NewRegistry(s rest.StandardStorage) Registry {
	return &storage{s}
}

func (s *storage) ListSchedulers(ctx api.Context, options *api.ListOptions) (*api.SchedulerList, error) {
	obj, err := s.List(ctx, options)
	if err != nil {
		return nil, err
	}

	return obj.(*api.SchedulerList), nil
}

func (s *storage) CreateScheduler(ctx api.Context, scheduler *api.Scheduler) error {
	_, err := s.Create(ctx, scheduler)
	return err
}

func (s *storage) UpdateScheduler(ctx api.Context, scheduler *api.Scheduler) error {
	_, _, err := s.Update(ctx, scheduler)
	return err
}

func (s *storage) WatchSchedulers(ctx api.Context, options *api.ListOptions) (watch.Interface, error) {
	return s.Watch(ctx, options)
}

func (s *storage) GetScheduler(ctx api.Context, name string) (*api.Scheduler, error) {
	obj, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return obj.(*api.Scheduler), nil
}

func (s *storage) DeleteScheduler(ctx api.Context, name string) error {
	_, err := s.Delete(ctx, name, nil)
	return err
}
