// Copyright 2017 The etcd-operator Authors
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

package e2e

import (
	"testing"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/Azure/etcd-operator/pkg/spec"
	"github.com/Azure/etcd-operator/test/e2e/e2eutil"
	"github.com/Azure/etcd-operator/test/e2e/framework"
)

func TestCreateClusterWithPV(t *testing.T) {

	f := framework.Global
	c := e2eutil.NewCluster("test-etcd-", 3)
	c.Spec.Pod = &spec.PodPolicy{
		PersistentVolumeClaimSpec: &v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse("512Mi")},
			},
			StorageClassName: func(s string) *string { return &s }("standard"),
		},
	}

	testEtcd, err := e2eutil.CreateCluster(t, f.CRClient, f.Namespace, c)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := e2eutil.DeleteCluster(t, f.CRClient, f.KubeClient, testEtcd); err != nil {
			t.Fatal(err)
		}
	}()

}
