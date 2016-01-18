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

package scheduler

// Note: if you change code in this file, you might need to change code in
// contrib/mesos/pkg/scheduler/.

import (
	"fmt"
	"time"

	"k8s.io/kubernetes/pkg/api"
	apierrors "k8s.io/kubernetes/pkg/api/errors"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/client/record"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/util"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/plugin/pkg/scheduler/metrics"

	"github.com/golang/glog"
)

// Binder knows how to write a binding.
type Binder interface {
	Bind(binding *api.Binding) error
}

// SystemModeler can help scheduler produce a model of the system that
// anticipates reality. For example, if scheduler has pods A and B both
// using hostPort 80, when it binds A to machine M it should not bind B
// to machine M in the time when it hasn't observed the binding of A
// take effect yet.
//
// Since the model is only an optimization, it's expected to handle
// any errors itself without sending them back to the scheduler.
type SystemModeler interface {
	// AssumePod assumes that the given pod exists in the system.
	// The assumtion should last until the system confirms the
	// assumtion or disconfirms it.
	AssumePod(pod *api.Pod)
	// ForgetPod removes a pod assumtion. (It won't make the model
	// show the absence of the given pod if the pod is in the scheduled
	// pods list!)
	ForgetPod(pod *api.Pod)
	ForgetPodByKey(key string)

	// For serializing calls to Assume/ForgetPod: imagine you want to add
	// a pod if and only if a bind succeeds, but also remove a pod if it is deleted.
	// TODO: if SystemModeler begins modeling things other than pods, this
	// should probably be parameterized or specialized for pods.
	LockedAction(f func())
}

// Scheduler watches for new unscheduled pods. It attempts to find
// nodes that they fit on and writes bindings back to the api server.
type Scheduler struct {
	config                         *Config
	kubeClient                     client.Interface
	clock                          util.Clock
	schedulerStatusUpdateFrequency time.Duration
	registerScheduler              bool
	registrationCompleted          bool
	schedulerName                  string
}

type Config struct {
	// It is expected that changes made via modeler will be observed
	// by NodeLister and Algorithm.
	Modeler    SystemModeler
	NodeLister algorithm.NodeLister
	Algorithm  algorithm.ScheduleAlgorithm
	Binder     Binder

	// NextPod should be a function that blocks until the next pod
	// is available. We don't use a channel for this, because scheduling
	// a pod may take some amount of time and we don't want pods to get
	// stale while they sit in a channel.
	NextPod func() *api.Pod

	// Error is called if there is an error. It is passed the pod in
	// question, and the error
	Error func(*api.Pod, error)

	// Recorder is the EventRecorder to use
	Recorder record.EventRecorder

	// Close this to shut down the scheduler.
	StopEverything chan struct{}
}

const (
	// schedulerStatusUpdateRetry specifies how many times scheduler retries when posting scheduler status failed.
	schedulerStatusUpdateRetry = 5
)

// New returns a new scheduler.
func New(c *Config, client client.Interface, schedulerName string) *Scheduler {
	s := &Scheduler{
		config:     c,
		kubeClient: client,
		clock:      util.RealClock{},
		schedulerStatusUpdateFrequency: 10 * time.Second,
		registerScheduler:              true,
		registrationCompleted:          false,
		schedulerName:                  schedulerName,
	}
	metrics.Register()
	return s
}

// Run begins watching and scheduling. It starts a goroutine and returns immediately.
func (s *Scheduler) Run() {
	go wait.Until(s.syncSchedulerStatus, s.schedulerStatusUpdateFrequency, wait.NeverStop)
	go wait.Until(s.scheduleOne, 0, s.config.StopEverything)
}

func (s *Scheduler) syncSchedulerStatus() {
	if s.kubeClient == nil {
		return
	}
	if s.registerScheduler {
		// This will exit immediately if it doesn't need to do anything.
		s.registerWithApiserver()
	}
	if err := s.updateSchedulerStatus(); err != nil {
		glog.Errorf("Unable to update node status: %v", err)
	}
}

func (s *Scheduler) registerWithApiserver() {
	if s.registrationCompleted {
		return
	}
	step := 100 * time.Millisecond
	for {
		time.Sleep(step)
		step = step * 2
		if step >= 7*time.Second {
			step = 7 * time.Second
		}

		scheduler, err := s.initialSchedulerStatus()
		if err != nil {
			glog.Errorf("Unable to construct api.Scheduler object: %v", err)
			continue
		}
		glog.V(2).Infof("Attempting to register scheduler %s", scheduler.Name)
		if _, err := s.kubeClient.Schedulers().Create(scheduler); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				glog.V(2).Infof("Unable to register %s with the apiserver: %v", scheduler.Name, err)
				continue
			}
			currentScheduler, err := s.kubeClient.Schedulers().Get(s.schedulerName)
			if err != nil {
				glog.Errorf("error getting scheduler %q: %v", s.schedulerName, err)
				continue
			}
			if currentScheduler == nil {
				glog.Errorf("no scheduler instance returned for %q", s.schedulerName)
				continue
			}
			if err := s.kubeClient.Schedulers().Delete(scheduler.Name); err != nil {
				glog.Errorf("Unable to delete old scheduler: %v", err)
			} else {
				glog.Errorf("Deleted old scheduler object %q", scheduler.Name)
			}
			continue
		}
		glog.Infof("Successfully registered scheduler %s", scheduler.Name)
		s.registrationCompleted = true
		return
	}
}

func (s *Scheduler) initialSchedulerStatus() (*api.Scheduler, error) {
	scheduler := &api.Scheduler{
		ObjectMeta: api.ObjectMeta{
			Name:   s.schedulerName,
			Labels: map[string]string{"kubernetes.io/scheduler": s.schedulerName},
		},
		Spec: api.SchedulerSpec{
			SchedulerType: "testType",
		},
	}

	if err := s.setSchedulerStatus(scheduler); err != nil {
		return nil, err
	}
	return scheduler, nil
}

func (s *Scheduler) updateSchedulerStatus() error {
	for i := 0; i < schedulerStatusUpdateRetry; i++ {
		if err := s.tryUpdateSchedulerStatus(); err != nil {
			glog.Errorf("Error updating scheduler status, will retry: %v", err)
		} else {
			return nil
		}
	}
	return fmt.Errorf("update scheduler status exceeds retry count")
}

func (s *Scheduler) tryUpdateSchedulerStatus() error {
	scheduler, err := s.kubeClient.Schedulers().Get(s.schedulerName)
	if err != nil {
		return fmt.Errorf("error getting scheduler %q: %v", s.schedulerName, err)
	}
	if scheduler == nil {
		return fmt.Errorf("no scheduler instance returned for %q", s.schedulerName)
	}
	if err := s.setSchedulerStatus(scheduler); err != nil {
		return err
	}
	// Update the current status on the API server
	_, err = s.kubeClient.Schedulers().UpdateStatus(scheduler)
	return err
}

func (s *Scheduler) setSchedulerStatus(scheduler *api.Scheduler) error {
	s.setSchedulerReadyCondition(scheduler)
	return nil
}

// Set Readycondition for the scheduler.
func (s *Scheduler) setSchedulerReadyCondition(scheduler *api.Scheduler) {
	currentTime := unversioned.NewTime(s.clock.Now())
	var newSchedulerReadyCondition api.SchedulerCondition
	newSchedulerReadyCondition = api.SchedulerCondition{
		Type:   api.SchedulerReady,
		Status: api.ConditionFalse,
		//		Reason:            "SchedulerNotReady",
		Reason:            "SchedulerReady",
		Message:           "SchedulerReady is posting ready status",
		LastHeartbeatTime: currentTime,
	}

	readyConditionUpdated := false
	for i := range scheduler.Status.Conditions {
		if scheduler.Status.Conditions[i].Type == api.SchedulerReady {
			scheduler.Status.Conditions[i] = newSchedulerReadyCondition
			readyConditionUpdated = true
			break
		}
	}
	if !readyConditionUpdated {
		scheduler.Status.Conditions = append(scheduler.Status.Conditions, newSchedulerReadyCondition)
	}
}

func (s *Scheduler) scheduleOne() {
	pod := s.config.NextPod()

	glog.V(3).Infof("Attempting to schedule: %+v", pod)
	start := time.Now()
	dest, err := s.config.Algorithm.Schedule(pod, s.config.NodeLister)
	if err != nil {
		glog.V(1).Infof("Failed to schedule: %+v", pod)
		s.config.Recorder.Eventf(pod, api.EventTypeWarning, "FailedScheduling", "%v", err)
		s.config.Error(pod, err)
		return
	}
	metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInMicroseconds(start))

	b := &api.Binding{
		ObjectMeta: api.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
		Target: api.ObjectReference{
			Kind: "Node",
			Name: dest,
		},
	}

	// We want to add the pod to the model if and only if the bind succeeds,
	// but we don't want to race with any deletions, which happen asynchronously.
	s.config.Modeler.LockedAction(func() {
		bindingStart := time.Now()
		err := s.config.Binder.Bind(b)
		if err != nil {
			glog.V(1).Infof("Failed to bind pod: %+v", err)
			s.config.Recorder.Eventf(pod, api.EventTypeNormal, "FailedScheduling", "Binding rejected: %v", err)
			s.config.Error(pod, err)
			return
		}
		metrics.BindingLatency.Observe(metrics.SinceInMicroseconds(bindingStart))
		s.config.Recorder.Eventf(pod, api.EventTypeNormal, "Scheduled", "Successfully assigned %v to %v", pod.Name, dest)
		// tell the model to assume that this binding took effect.
		assumed := *pod
		assumed.Spec.NodeName = dest
		s.config.Modeler.AssumePod(&assumed)
	})

	metrics.E2eSchedulingLatency.Observe(metrics.SinceInMicroseconds(start))
}
