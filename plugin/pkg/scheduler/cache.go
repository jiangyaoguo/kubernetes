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

import (
	//	"fmt"
	"github.com/golang/groupcache/lru"

	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/util/sets"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm"
	"k8s.io/kubernetes/plugin/pkg/scheduler/algorithm/predicates"
)

const maxCacheEntries = 4096

//// Return unique key for equivalence class
//type EquivalencePod interface {
//	GenerateEquivalenceKey(pod *api.Pod) (string, error)
//}

type PodFitInfo struct {
	Fit        bool
	FailReason sets.String
}

type NodeCache struct {
	cache *lru.Cache
}

func NewNodeCache() *lru.Cache {
	return lru.New(maxCacheEntries)
}

// Store a map of predicate cache with maxsize
type PredicateCache struct {
	equivalent EquivalencePod
	nodesCache map[string]NodeCache
}

func NewPredicateCache(equaivalentPod EquivalencePod) *PredicateCache {
	return &PredicateCache{
		equivalent: equaivalentPod,
	}
}

func (pc *PredicateCache) AddPodPredicates(pod *api.Pod, fitNodeList *api.NodeList, failedPredicates *FailedPredicateMap) error {
	equivalenceHash := HashEquivalencePod(pod)

	for node := range fitNodeList.Items {
		if _, exist := nodesCache.get(node.ObjectMeta.Name); exist {
			nodesCache[node.ObjectMeta.Name].Add(key, PodFitInfo{Fit: true, FailReason: ""})
		}
		pc.nodesCache[node.ObjectMeta.Name] = NodeCache{Fit: true, FailReason: ""}
	}
	for _, fail := range failedPredicates {
		pc.nodesCache[key] = NodeCache{Fit: false, FailReason: fail}
	}

	return err
}

func (pc *PredicateCache) UpdatePodPredicates(pod *api.Pod, fitNodeList *api.NodeList, failedPredicates *FailedPredicateMap) error {
	if key, err := pc.equivalent.GenerateEquivalenceKey(pod); err == nil {
		pc.cache.Add(key, podPredicate{
			fitNodeList:      fitNodeList,
			failedPredicates: failedPredicates,
		})
		return nil
	}
	return err
}

func (pc *PredicateCache) GetCachedPredicates(pod *api.Pod, nodes api.NodeList) (api.NodeList, api.NodeList, FailedPredicateMap, error) {
	fitNodeList := api.NodeList{}
	noCacheNodeList := api.NodeList{}
	failedPredicates := FailedPredicateMap{}

	// for mock
	noCacheNodeList = nodes
	return fitNodeList, noCacheNodeList, failedPredicates
	//	err error
	//	if key, err = pc.equivalent.GenerateEquivalenceKey(pod); err == nil {
	//		cachePredicate, ok := pc.cache.Get(key)
	//		if ok {
	//			cacheResult, ok := cachePredicate.(podPredicate)
	//			if ok {
	//				fitNodeList = &cacheResult.fitNodeList
	//				failedPredicates = &cacheResult.failedPredicates
	//			}
	//		}
	//	} else {
	//		return &fitNodeList, &noCacheNodeList, &failedPredicates, err
	//	}

	//	return &fitNodeList, &noCacheNodeList, &failedPredicates, nil
}

func (pc *PredicateCache) InvalidNodeCache(node *api.Node) error {
	return nil
}

// HashEquivalencePod returns the hash of the pod.
func HashEquivalencePod(pod *api.Pod) uint64 {
	equivalencePod := extractEqivalancePod(pod)
	hash := adler32.New()
	util.DeepHashObject(hash, *equivalencePod)
	return uint64(hash.Sum32())
}

func extractEqivalancePod(pod *api.Pod) *algorithm.EquivalencePod {
	equivalencePod := EquivalencePod{}
	podSpec := &(pod.Spec)
	for vol := range podSpec.Volumes {
		append(equivalencePod.Volumes, vol)
	}
	equivalencePod.NodeSelector = podSpec.NodeSelector
	equivalencePod.Request = predicates.GetResourceRequest(pod)
	equivalencePod.Ports = predicates.GetUsedPorts(pod)
	return &equivalencePod
}

type EquivalencePod struct {
	Volumes      []api.Volume
	NodeSelector map[string]string
	Ports        map[int]bool
	Request      predicates.ResourceRequest
}
