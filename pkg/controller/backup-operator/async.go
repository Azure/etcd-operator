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

package controller

import (
	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"

	"github.com/sirupsen/logrus"
)

// Note BackupStatus returned here is from the first round run
func (b *Backup) run(spec *api.BackupSpec) (*api.BackupStatus, error) {

}

func (b *Backup) handleBackup(spec *api.BackupSpec) (*api.BackupStatus, error) {
	switch spec.StorageType {
	case api.BackupStorageTypeS3:
		bs, err := handleS3(b.kubecli, spec.S3, spec.EtcdEndpoints, spec.ClientTLSSecret, b.namespace)
		if err != nil {
			return nil, err
		}
		return bs, nil
	case api.BackupStorageTypeABS:
		bs, err := handleABS(b.kubecli, spec.ABS, spec.EtcdEndpoints, spec.ClientTLSSecret, b.namespace)
		if err != nil {
			return nil, err
		}
		return bs, nil
	default:
		logrus.Fatalf("unknown StorageType: %v", spec.StorageType)
	}
	return nil, nil
}
