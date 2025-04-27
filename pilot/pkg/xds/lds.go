// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xds

import (
	"bytes"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/util/protoconv"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/util/sets"
)

type LdsGenerator struct {
	Server *DiscoveryServer
}

var _ model.XdsResourceGenerator = &LdsGenerator{}

// Map of all configs that do not impact LDS
var skippedLdsConfigs = map[model.NodeType]sets.Set[kind.Kind]{
	model.Router: sets.New[kind.Kind](
		// for autopassthrough gateways, we build filterchains per-dr subset
		kind.WorkloadGroup,
		kind.WorkloadEntry,
		kind.Secret,
		kind.ProxyConfig,
	),
	model.SidecarProxy: sets.New[kind.Kind](
		kind.Gateway,
		kind.WorkloadGroup,
		kind.WorkloadEntry,
		kind.Secret,
		kind.ProxyConfig,
	),
	model.Waypoint: sets.New[kind.Kind](
		kind.Gateway,
		kind.WorkloadGroup,
		kind.WorkloadEntry,
		kind.Secret,
		kind.ProxyConfig,
	),
}

func ldsNeedsPush(proxy *model.Proxy, req *model.PushRequest) bool {
	if req == nil {
		return true
	}
	switch proxy.Type {
	case model.Waypoint:
		if model.HasConfigsOfKind(req.ConfigsUpdated, kind.Address) {
			// Waypoint proxies have a matcher against pod IPs in them. Historically, any LDS change would do a full
			// push, recomputing push context. Doing that on every IP change doesn't scale, so we need these to remain
			// incremental pushes.
			// This allows waypoints only to push LDS on incremental pushes to Address type which would otherwise be skipped.
			return true
		}
		// Otherwise, only handle full pushes (skip endpoint-only updates)
		if !req.Full {
			return false
		}
	default:
		if !req.Full {
			// LDS only handles full push
			return false
		}
	}
	// If none set, we will always push
	if len(req.ConfigsUpdated) == 0 {
		return true
	}
	for config := range req.ConfigsUpdated {
		if !skippedLdsConfigs[proxy.Type].Contains(config.Kind) {
			return true
		}
	}
	return false
}

func (l LdsGenerator) Generate(proxy *model.Proxy, _ *model.WatchedResource, req *model.PushRequest) (model.Resources, model.XdsLogDetails, error) {
	if !ldsNeedsPush(proxy, req) {
		return nil, model.DefaultXdsLogDetails, nil
	}
	resources := model.Resources{}
	// Modified by Higress
	listeners, logs := l.Server.ConfigGenerator.BuildListeners(proxy, req)
	listenersExt, _ := l.Server.ConfigGenerator.BuildListenersExt(proxy, req)
	for i, c := range listeners {
		if i >= len(listenersExt) {
			log.Warnf("listenersExt is not enough, i: %d, len(listenersExt): %d", i, len(listenersExt))
		}
		res := protoconv.MessageToAny(c)
		resExt := protoconv.MessageToAny(listenersExt[i])
		if res == nil {
			log.Warnf("failed to convert listener to any, listener: %v", c)
			continue
		}
		if resExt == nil {
			log.Warnf("failed to convert listenerExt to any, listenerExt: %v", listenersExt[i])
			continue
		}
		// check if res equals resExt
		if res.TypeUrl != resExt.TypeUrl || bytes.Equal(res.Value, resExt.Value) == false {
			log.Warnf("res != resExt, res: %v, resExt: %v", res, resExt)
		}
		resources = append(resources, &discovery.Resource{
			Name:     c.Name,
			Resource: res,
		})
	}
	return resources, logs, nil
	// End modified by Higress
}
