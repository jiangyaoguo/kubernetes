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
	"fmt"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/validation"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/registry/generic"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util/validation/field"
)

// schedulerStrategy implements behavior for schedulers
type schedulerStrategy struct {
	runtime.ObjectTyper
	api.NameGenerator
}

// Schedulers is the default logic that applies when creating and updating Scheduler
// objects.
var Strategy = schedulerStrategy{api.Scheme, api.SimpleNameGenerator}

// NamespaceScoped is false for scheduler.
func (schedulerStrategy) NamespaceScoped() bool {
	return false
}

// AllowCreateOnUpdate is false for schedulers.
func (schedulerStrategy) AllowCreateOnUpdate() bool {
	return false
}

// PrepareForCreate clears fields that are not allowed to be set by end users on creation.
func (schedulerStrategy) PrepareForCreate(obj runtime.Object) {
	_ = obj.(*api.Scheduler)
	// Schedulers allow *all* fields, including status, to be set on create.
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
func (schedulerStrategy) PrepareForUpdate(obj, old runtime.Object) {
	newScheduler := obj.(*api.Scheduler)
	oldScheduler := old.(*api.Scheduler)
	newScheduler.Status = oldScheduler.Status
}

// Validate validates a new scheduler.
func (schedulerStrategy) Validate(ctx api.Context, obj runtime.Object) field.ErrorList {
	scheduler := obj.(*api.Scheduler)
	return validation.ValidateScheduler(scheduler)
	//	return nil
}

// Canonicalize normalizes the object after validation.
func (schedulerStrategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate is the default update validation for an end user.
func (schedulerStrategy) ValidateUpdate(ctx api.Context, obj, old runtime.Object) field.ErrorList {
	errorList := validation.ValidateScheduler(obj.(*api.Scheduler))
	return append(errorList, validation.ValidateSchedulerUpdate(obj.(*api.Scheduler), old.(*api.Scheduler))...)
	//	return nil
}

func (schedulerStrategy) AllowUnconditionalUpdate() bool {
	return true
}

func (ns schedulerStrategy) Export(obj runtime.Object, exact bool) error {
	n, ok := obj.(*api.Scheduler)
	if !ok {
		// unexpected programmer error
		return fmt.Errorf("unexpected object: %v", obj)
	}
	ns.PrepareForCreate(obj)
	if exact {
		return nil
	}
	// Schedulers are the only resources that allow direct status edits, therefore
	// we clear that without exact so that the scheduler value can be reused.
	n.Status = api.SchedulerStatus{}
	return nil
}

type schedulerStatusStrategy struct {
	schedulerStrategy
}

var StatusStrategy = schedulerStatusStrategy{Strategy}

func (schedulerStatusStrategy) PrepareForCreate(obj runtime.Object) {
	_ = obj.(*api.Scheduler)
	// Schedulers allow *all* fields, including status, to be set on create.
}

func (schedulerStatusStrategy) PrepareForUpdate(obj, old runtime.Object) {
	//	newScheduler := obj.(*api.Scheduler)
	//	oldScheduler := old.(*api.Scheduler)
	//	newScheduler.Spec = oldScheduler.Spec
}

func (schedulerStatusStrategy) ValidateUpdate(ctx api.Context, obj, old runtime.Object) field.ErrorList {
	//	return validation.ValidateSchedulerUpdate(obj.(*api.Scheduler), old.(*api.Scheduler))
	return nil
}

// Canonicalize normalizes the object after validation.
func (schedulerStatusStrategy) Canonicalize(obj runtime.Object) {
}

// NodeToSelectableFields returns a field set that represents the object.
func SchedulerToSelectableFields(scheduler *api.Scheduler) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(scheduler.ObjectMeta, false)
	return objectMetaFieldsSet
}

// MatchNode returns a generic matcher for a given label and field selector.
func MatchScheduler(label labels.Selector, field fields.Selector) generic.Matcher {
	return &generic.SelectionPredicate{
		Label: label,
		Field: field,
		GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
			schedulerObj, ok := obj.(*api.Scheduler)
			if !ok {
				return nil, nil, fmt.Errorf("not a scheduler")
			}
			return labels.Set(schedulerObj.ObjectMeta.Labels), SchedulerToSelectableFields(schedulerObj), nil
		},
	}
}
