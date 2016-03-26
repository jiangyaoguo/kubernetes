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
	"time"

	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	"k8s.io/kubernetes/pkg/client/cache"
	client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/framework"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/util"
	"k8s.io/kubernetes/pkg/util/wait"
	"k8s.io/kubernetes/pkg/watch"
)

const (
	schedulerTypeFeild = "scheduler.alpha.kubernetes.io/type"
	schedulerIdFeild   = "scheduler.alpha.kubernetes.io/name"
	// schedulerStatusUpdateRetry controls the number of retries of writing SchedulerStatus update.
	schedulerStatusUpdateRetry = 5
)

type schedulerPool struct {
	schedulerType string
	schedulerList []string
	index         int
}

type SchedulerController struct {
	kubeClient *client.Client
	// queue for pods that need scheduling
	PodQueue             *cache.FIFO
	UnScheduledPodLister cache.StoreToPodLister
	// scheduler store and controller
	schedulerController *framework.Controller
	schedulerStore      cache.StoreToScheduler
	// store all available scheduler
	schedulerCluster   map[string]*schedulerPool
	coolDownSchedulers []string
	schedulerNum       int
	schedulerlock      sync.RWMutex
	coolDownlock       sync.RWMutex

	now func() unversioned.Time

	NextPod func() *api.Pod
	// Close this to stop all reflectors
	StopEverything chan struct{}
	// Rate limiter for assign pods to scheduler
	AssignPodsRateLimiter util.RateLimiter

	schedulerMonitorPeriod time.Duration
	// If scheduler has no heartbeat for schedulerLostPeriod, it will be considered to be lost.
	schedulerLostPeriod time.Duration
	// Once a scheduler was lost, it wouldn't be registered after schedulerCoolDownPeriod.
	schedulerCoolDownPeriod time.Duration
}

func NewSchedulerController(client *client.Client) *SchedulerController {
	sc := &SchedulerController{
		kubeClient: client,
		PodQueue:   cache.NewFIFO(cache.MetaNamespaceKeyFunc),
		UnScheduledPodLister: cache.StoreToPodLister{
			Store: cache.NewStore(cache.MetaNamespaceKeyFunc),
		},
		schedulerCluster:        make(map[string]*schedulerPool),
		AssignPodsRateLimiter:   util.NewTokenBucketRateLimiter(50.0, 100),
		StopEverything:          make(chan struct{}),
		schedulerMonitorPeriod:  2 * time.Second,
		schedulerLostPeriod:     15 * time.Second,
		schedulerCoolDownPeriod: 5 * time.Second,
	}
	// Watch and queue pods that need scheduling.
	cache.NewReflector(sc.createUnassignedPodLW(), &api.Pod{}, sc.PodQueue, 0).RunUntil(sc.StopEverything)

	cache.NewReflector(sc.createUnassignedPodLW(), &api.Pod{}, sc.UnScheduledPodLister.Store, 0).RunUntil(sc.StopEverything)

	sc.schedulerStore.Store, sc.schedulerController = framework.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options api.ListOptions) (runtime.Object, error) {
				return sc.kubeClient.Schedulers().List(options)
			},
			WatchFunc: func(options api.ListOptions) (watch.Interface, error) {
				return sc.kubeClient.Schedulers().Watch(options)
			},
		},
		&api.Scheduler{},
		controller.NoResyncPeriodFunc(),
		framework.ResourceEventHandlerFuncs{
		//AddFunc:    sc.AddAvailableScheduler,
		//DeleteFunc: sc.RemoveScheduler,
		},
	)

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
	selector := fields.ParseSelectorOrDie("spec.nodeName==" + "" + ",status.phase!=" + string(api.PodSucceeded) + ",status.phase!=" + string(api.PodFailed))
	return cache.NewListWatchFromClient(sc.kubeClient, "pods", api.NamespaceAll, selector)
}

func (sc *SchedulerController) Run() {
	go wait.Until(sc.assignOne, 0, sc.StopEverything)

	go wait.Until(func() {
		if err := sc.monitorSchedulerStatus(); err != nil {
			glog.Errorf("Error monitoring scheduler status: %v", err)
		}
	}, sc.schedulerMonitorPeriod, wait.NeverStop)

	go wait.Until(sc.clearPodsOnLostScheduler, sc.schedulerMonitorPeriod, sc.StopEverything)
}

func (sc *SchedulerController) assignOne() {
	glog.Errorf("SchedulerController ready to get pod")
	pod := sc.nextPod()
	needAssign := needAssignScheduler(pod)
	for !needAssign {
		pod := sc.nextPod()
		needAssign = needAssignScheduler(pod)
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

func needAssignScheduler(pod *api.Pod) bool {
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
	sc.schedulerlock.Lock()
	defer sc.schedulerlock.Unlock()

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
	pod, err := sc.kubeClient.Pods(pod.Namespace).Update(pod)
	if err != nil {
		return err
	}
	return nil
}

func (sc *SchedulerController) AddAvailableScheduler(obj interface{}) {
	scheduler, ok := obj.(*api.Scheduler)
	if !ok {
		return
	}

	sc.schedulerlock.Lock()
	defer sc.schedulerlock.Unlock()

	if spl, exist := sc.schedulerCluster[scheduler.Spec.SchedulerType]; exist {
		spl.schedulerList = append(spl.schedulerList, scheduler.Name)

	} else {
		sc.schedulerCluster[scheduler.Spec.SchedulerType] = &schedulerPool{
			schedulerType: scheduler.Spec.SchedulerType,
			schedulerList: []string{scheduler.Name},
			index:         0,
		}
	}
	sc.schedulerNum = sc.schedulerNum + 1
	glog.Infof("Add new scheduler %s", scheduler.Name)
}

func (sc *SchedulerController) RemoveScheduler(obj interface{}) error {
	scheduler, ok := obj.(*api.Scheduler)
	if !ok {
		return fmt.Errorf("Receive unkonwn object when remove scheduler.")
	}

	sc.schedulerlock.Lock()
	defer sc.schedulerlock.Unlock()

	if spl, exist := sc.schedulerCluster[scheduler.Spec.SchedulerType]; exist {
		for index, id := range spl.schedulerList {
			if id == scheduler.Name {
				spl.schedulerList = append(spl.schedulerList[:index], spl.schedulerList[index+1:]...)
			}
		}
		sc.schedulerNum = sc.schedulerNum - 1
		glog.Infof("Remove scheduler %s", scheduler.Name)

		//Clean up pods scheduled to the lost scheduler.

		return nil
	} else {
		return fmt.Errorf("Remove scheduler: %s with non-exist type: %s", scheduler.Name, scheduler.Spec.SchedulerType)
	}

	// Add scheduler to gc targets.
	sc.coolDownlock.Lock()
	sc.coolDownSchedulers = append(sc.coolDownSchedulers, scheduler)
	sc.coolDownlock.Unlock()

}

func (sc *SchedulerController) monitorSchedulerStatus() error {
	schedulers, err := sc.kubeClient.Schedulers().List(api.ListOptions{})
	if err != nil {
		return err
	}
	for i := range schedulers.Items {
		if !sc.hasScheduler(schedulers.Items[i].Name) {
			glog.Infof("SchedulerController observed a new scheduler: %v", schedulers.Items[i])
			//sc.AddAvailableScheduler(scheduler.Items[i])
		}
	}

	for i := range schedulers.Items {
		scheduler := &schedulers.Items[i]

		statusChanged := false
		rep := 0
		for ; rep < schedulerStatusUpdateRetry; rep++ {
			if scheduler.Status.Phase == api.SchedulerRunning {
				for i := range scheduler.Status.Conditions {
					if time.Now().Sub(scheduler.Status.Conditions[i].LastHeartbeatTime.Time) >= sc.schedulerLostPeriod {
						scheduler.Status.Phase = api.SchedulerTerminated
						if sc.hasScheduler(scheduler.Name) {
							sc.RemoveScheduler(scheduler)
						}
						statusChanged = true
					}
				}
			} else {
				for i := range scheduler.Status.Conditions {
					lastLiveTime := scheduler.Status.Conditions[i].LastHeartbeatTime.Time
					checkLiveTime := time.Now()
					if scheduler.Status.Conditions[i].Type == api.SchedulerReady && checkLiveTime.Sub(lastLiveTime) < sc.schedulerLostPeriod {
						if scheduler.Status.Phase != api.SchedulerRunning {
							scheduler.Status.Phase = api.SchedulerRunning
							if !sc.hasScheduler(scheduler.Name) {
								sc.AddAvailableScheduler(scheduler)
							}
							statusChanged = true
							break
						}
					}
				}

			}
			if statusChanged {
				_, err = sc.kubeClient.Schedulers().UpdateStatus(scheduler)
				if err == nil {
					glog.Infof("Update scheduler %v status (%v) succesfully", scheduler.Name, scheduler.Status.Phase)
					break
				} else {
					glog.Errorf("Update scheduler %v status (%v) failed: %v", scheduler.Name, scheduler.Status.Phase, err.Error())
				}
			}
		}
		if rep > schedulerStatusUpdateRetry {
			glog.Errorf("After %v tries, Update scheduler %v status (%v) still failed. Skip update.", rep, scheduler.Name, scheduler.Status.Phase)
		}
	}
	return nil
}

func (sc *SchedulerController) hasScheduler(name string) bool {
	exsit := false
	for _, schedulerSets := range sc.schedulerCluster {
		for _, scheduler := range schedulerSets.schedulerList {
			if scheduler == name {
				exsit = true
				break
			}
		}
	}
	return exsit
}

func (sc *SchedulerController) getSchedulerNum() int {
	sc.schedulerlock.Lock()
	defer sc.schedulerlock.Unlock()

	return sc.schedulerNum
}

func (sc *SchedulerController) clearPodsOnLostScheduler() {
	pods, err := sc.UnScheduledPodLister.List(labels.Everything())
	if err != nil {
		return
	}
	for _, pod := range pods {
		if !needAssignScheduler(pod) {
			scheduler := pod.Annotations[schedulerIdFeild]
			sc.coolDownlock.Lock()
			lostSchedulers := sc.coolDownSchedulers
			sc.coolDownlock.Unlock()
			for _, lost := range lostSchedulers {
				if scheduler == lost {
					sc.assignPodToScheduler(pod, "")
					break
				}
			}
		}
	}
}
