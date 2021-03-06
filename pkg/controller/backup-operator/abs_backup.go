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
	"crypto/tls"
	"fmt"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup"
	"github.com/coreos/etcd-operator/pkg/backup/writer"
	"github.com/coreos/etcd-operator/pkg/util/azureutil/absfactory"
	"github.com/coreos/etcd-operator/pkg/util/etcdutil"
	"github.com/coreos/etcd-operator/pkg/util/k8sutil"

	"k8s.io/client-go/kubernetes"
)

// TODO: replace this with generic backend interface for other options (PV, Azure)
// handleABS saves etcd cluster's backup to specificed ABS path.
func handleABS(kubecli kubernetes.Interface, s *api.ABSBackupSource, sch api.BackupSchedule, endpoints []string, clientTLSSecret, namespace string) (*api.BackupStatus, error) {
	cli, err := absfactory.NewClientFromSecret(kubecli, namespace, s.ABSSecret)
	if err != nil {
		return nil, err
	}

	var tlsConfig *tls.Config
	if len(clientTLSSecret) != 0 {
		d, err := k8sutil.GetTLSDataFromSecret(kubecli, namespace, clientTLSSecret)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS data from secret (%v): %v", clientTLSSecret, err)
		}
		tlsConfig, err = etcdutil.NewTLSConfig(d.CertData, d.KeyData, d.CAData)
		if err != nil {
			return nil, fmt.Errorf("failed to constructs tls config: %v", err)
		}
	}

	bm := backup.NewBackupManagerFromWriter(kubecli, writer.NewABSWriter(cli.ABS), tlsConfig, endpoints, namespace)
	appendRev := false
	if sch.BackupIntervalInSecond > 0 {
		appendRev = true
	}
	rev, etcdVersion, err := bm.SaveSnap(s.Path, appendRev)
	if err != nil {
		return nil, fmt.Errorf("failed to save snapshot (%v)", err)
	}

	err = bm.PurgeBackup(s.Path, sch.MaxBackups)
	if err != nil {
		return nil, fmt.Errorf("failed to purge backups (%v)", err)
	}
	return &api.BackupStatus{EtcdVersion: etcdVersion, EtcdRevision: rev}, nil
}
