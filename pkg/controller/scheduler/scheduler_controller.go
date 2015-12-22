/*
Copyright 2015 The Kubernetes Authors All rights reserved.

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
	"sync"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/util"

	"github.com/golang/glog"
)

const (
	schedulerTypeFeild = "scheduler.alpha.kubernetes.io/type"
	schedulerIdFeild   = "scheduler.alpha.kubernetes.io/name"
)

type schedulerPool struct {
	schedulerType string
	schedulerList []string
	index         int
}

type SchedulerController struct {
	Client *client.Client
	// queue for pods that need scheduling
	PodQueue *cache.FIFO
	// store all available scheduler
	schedulerCluster map[string]*schedulerPool
	lock             sync.RWMutex

	NextPod func() *api.Pod
	// Close this to stop all reflectors
	StopEverything chan struct{}
	// Rate limiter for assign pods to scheduler
	AssignPodsRateLimiter util.RateLimiter
}

func NewSchedulerController(client *client.Client) *SchedulerController {
	sc := &SchedulerController{
		Client:                client,
		PodQueue:              cache.NewFIFO(cache.MetaNamespaceKeyFunc),
		schedulerCluster:      make(map[string]*schedulerPool),
		AssignPodsRateLimiter: util.NewTokenBucketRateLimiter(50.0, 100),
		StopEverything:        make(chan struct{}),
	}
	// Watch and queue pods that need scheduling.
	cache.NewReflector(sc.createUnassignedPodLW(), &api.Pod{}, sc.PodQueue, 0).RunUntil(sc.StopEverything)

	// Add fake scheduler
	sc.RegistryScheduler("faketype", "fakescheduler1")
	sc.RegistryScheduler("faketype", "fakescheduler2")
	sc.RegistryScheduler("faketype", "fakescheduler3")

	return sc
}

func (sc *SchedulerController) nextPod() *api.Pod {
	pod := sc.PodQueue.Pop().(*api.Pod)
	glog.V(2).Infof("About to try and find scheduler for pod %v", pod.Name)
	return pod
}

// Returns a cache.ListWatch that finds all pods that need to be
// scheduled.
// TODO: combine this with configfactory.createUnassignedPodLW in /plugin/scheduler/factory package
func (sc *SchedulerController) createUnassignedPodLW() *cache.ListWatch {
	return cache.NewListWatchFromClient(sc.Client, "pods", api.NamespaceAll, fields.Set{client.PodHost: ""}.AsSelector())
}

func (sc *SchedulerController) Run() {
	go util.Until(sc.assignOne, 0, sc.StopEverything)
}

func (sc *SchedulerController) assignOne() {
	glog.Errorf("SchedulerController ready to get pod")
	pod := sc.nextPod()
	needAssign := sc.needAssignScheduler(pod)
	for !needAssign {
		pod := sc.nextPod()
		needAssign = sc.needAssignScheduler(pod)
	}
	if sc.AssignPodsRateLimiter != nil {
		sc.AssignPodsRateLimiter.Accept()
	}

	schedulerId, err := sc.nextScheduler(pod)
	if err != nil {
		glog.Errorf("Assign scheduler failed: %v", err.Error())
	}
	err = sc.assignPodToScheduler(pod, schedulerId)
	if err != nil {
		glog.Errorf("Assign pod %v to scheduler %v failed: %v", pod.Name, schedulerId, err.Error())
	} else {
		glog.Errorf("Assign pod %v to scheduler %v successfully", pod.Name, schedulerId)
	}
}

func (sc *SchedulerController) needAssignScheduler(pod *api.Pod) bool {
	if _, exist := pod.Annotations[schedulerTypeFeild]; !exist {
		return false
	}
	schedulerId, exist := pod.Annotations[schedulerIdFeild]
	if !exist {
		glog.V(2).Infof("Pod %v has no schedulerId feild", pod.Name)
		return true
	}
	if schedulerId == "" {
		glog.V(2).Infof("Pod %v has not been assigned to any scheduler", pod.Name)
		return true
	}
	return false
}

func (sc *SchedulerController) nextScheduler(pod *api.Pod) (string, error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	schedulerType, exist := pod.Annotations[schedulerTypeFeild]
	if !exist {
		return "", fmt.Errorf("No scheduler type for pod %v", pod.Name)
	}
	if spl, exist := sc.schedulerCluster[schedulerType]; exist {
		spl.index = spl.index % len(spl.schedulerList)
		schedulerId := spl.schedulerList[spl.index]
		spl.index = spl.index + 1
		return schedulerId, nil
	} else {
		return "", fmt.Errorf("No available scheduler for type %s", schedulerType)
	}
}

func (sc *SchedulerController) assignPodToScheduler(pod *api.Pod, schedulerId string) error {
	// update schedulerId in anotaion
	pod.Annotations[schedulerIdFeild] = schedulerId
	pod, err := sc.Client.Pods(pod.Namespace).Update(pod)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SchedulerController) RegistryScheduler(schedulerType string, schedulerId string) error {
	if spl, exist := sc.schedulerCluster[schedulerType]; exist {
		spl.schedulerList = append(spl.schedulerList, schedulerId)
	} else {
		sc.schedulerCluster[schedulerType] = &schedulerPool{
			schedulerType: schedulerType,
			schedulerList: []string{schedulerId},
			index:         0,
		}
	}
	return nil
}
func (sc *SchedulerController) RemoveScheduler(schedulerType string, schedulerId string) error {
	if spl, exist := sc.schedulerCluster[schedulerType]; exist {
		for index, id := range spl.schedulerList {
			if id == schedulerId {
				spl.schedulerList = append(spl.schedulerList[:index], spl.schedulerList[index+1:]...)
			}
		}
		return nil
	} else {
		return fmt.Errorf("Remove scheduler: %s with non-exist type: %s", schedulerId, schedulerType)
	}
}
