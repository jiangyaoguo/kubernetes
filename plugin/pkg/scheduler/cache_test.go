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
	//	"fmt"
	"reflect"
	"testing"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm/predicates"
	schedulerapi "k8s.io/kubernetes/plugin/pkg/scheduler/api"
)

func generatePod(name, cpu, mem string) *api.Pod {
	return &api.Pod{
		ObjectMeta: api.ObjectMeta{
			Name: name,
		},
		Spec: api.PodSpec{
			Containers: []api.Container{{
				Name:  "pause",
				Image: "gcr.io/google_containers/pause:1.0",
				Resources: api.ResourceRequirements{
					Requests: api.ResourceList{
						api.ResourceCPU:    resource.MustParse(cpu),
						api.ResourceMemory: resource.MustParse(mem),
					},
				},
			}},
		},
	}
}

type fakeEquivalencePod struct {
	request predicates.ResourceRequest
}

func fakeGetEquivalencePodFunc(pod *api.Pod) interface{} {
	return &fakeEquivalencePod{request: predicates.GetResourceRequest(pod)}
}

func TestAddCache(t *testing.T) {
	pod1 := generatePod("pod1", "800m", "800Mi")
	pod2 := generatePod("pod2", "800m", "800Mi")
	//	pod3 := generatePod("pod3", "100m", "300Mi")
	//	pod4 := generatePod("pod4", "800m", "800Mi")

	nodes := makeNodeList([]string{"node1", "node2", "node3"})

	cache := NewSchedulerCache(fakeGetEquivalencePodFunc)

	// fake schedule result for pod1
	fitNodeList1 := makeNodeList([]string{"node1", "node2"})
	failedNode := sets.String{}
	failedNode.Insert("Node didn't have enough resource")
	failedPredicate1 := FailedPredicateMap{"node3": failedNode}
	hostPriority1 := []schedulerapi.HostPriority{{"machine1", 1}, {"machine2", 2}}

	cache.AddPodPredicatesCache(pod1, &fitNodeList1, &failedPredicate1)
	cache.AddPodPrioritiesCache(pod1, hostPriority1)

	//try to get cache date when scheduler pod2
	fitNodeList2, failedPredicate2, noCacheNodes := cache.GetCachedPredicates(pod2, nodes)
	expectFitNodeList := makeNodeList([]string{"node1", "node2"})
	expectFailedPredicate := FailedPredicateMap{"node3": failedNode}
	expectNoCacheNode := api.NodeList{}
	if !(reflect.DeepEqual(fitNodeList2, expectFitNodeList) && reflect.DeepEqual(failedPredicate2, expectFailedPredicate)) {
		t.Errorf("Get cached predicate for pod: %v error: wanted %v and %v, got %v and %v", pod2.Name, expectFitNodeList, expectFailedPredicate, fitNodeList2, failedPredicate2)
	}
	if !reflect.DeepEqual(noCacheNodes, expectNoCacheNode) {
		t.Errorf("Get no predicate cache nodes for pod: %v error: wanted %v, get %v", pod2.Name, expectNoCacheNode, noCacheNodes)
	}

	expectHostPriority := []schedulerapi.HostPriority{{"machine1", 1}, {"machine2", 2}}
	hostPriority2, noCacheNodes := cache.GetCachedPriorities(pod2, nodes)
	if !(reflect.DeepEqual(hostPriority2, expectHostPriority)) {
		t.Errorf("Get cached priority for pod: %v error: wanted %v , got %v", pod2.Name, expectHostPriority, hostPriority2)
	}
}
