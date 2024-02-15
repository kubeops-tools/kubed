/*
Copyright The Config Syncer Authors.

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

package syncer

import (
	"context"
	"encoding/base64"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	clientcmd_util "kmodules.xyz/client-go/tools/clientcmd"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	ConfigSyncKey      = "kubed.appscode.com/sync"
	ConfigOriginKey    = "kubed.appscode.com/origin"
	ConfigSyncContexts = "kubed.appscode.com/sync-contexts"

	OriginNameLabelKey      = "kubed.appscode.com/origin.name"
	OriginNamespaceLabelKey = "kubed.appscode.com/origin.namespace"
	OriginClusterLabelKey   = "kubed.appscode.com/origin.cluster"
)

type ConfigSyncer struct {
	kubeClient kubernetes.Interface
	recorder   record.EventRecorder

	clusterName string
	contexts    map[string]clusterContext
	lock        sync.RWMutex
}

func New(kc kubernetes.Interface, recorder record.EventRecorder) *ConfigSyncer {
	return &ConfigSyncer{
		kubeClient: kc,
		recorder:   recorder,
	}
}

func (s *ConfigSyncer) Configure(clusterName string, kubeconfigFile string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.clusterName = clusterName
	s.contexts = map[string]clusterContext{}

	// Parse external kubeconfig file, assume that it doesn't include source cluster
	if kubeconfigFile != "" {
		kConfig, err := clientcmd.LoadFromFile(kubeconfigFile)
		if err != nil {
			return errors.Errorf("failed to parse context list. Reason: %v", err)
		}

		for contextName := range kConfig.Contexts {
			ctx := clusterContext{}

			cfg, err := clientcmd_util.BuildConfigFromContext(kubeconfigFile, contextName)
			if err != nil {
				continue
			}
			if ctx.Client, err = kubernetes.NewForConfig(cfg); err != nil {
				continue
			}
			if ctx.Namespace, err = clientcmd_util.NamespaceFromContext(kubeconfigFile, contextName); err != nil {
				continue
			}
			ctx.Address = base64.StdEncoding.EncodeToString([]byte(cfg.Host))
			s.contexts[contextName] = ctx
		}
	}
	return nil
}

type clusterContext struct {
	Client    kubernetes.Interface
	Namespace string
	Address   string
}

func (s *ConfigSyncer) SyncIntoNamespace(namespace string) error {
	ns, err := s.kubeClient.CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{})
	if err != nil {
		return err
	}

	configMaps, err := s.kubeClient.CoreV1().ConfigMaps(core.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, configMap := range configMaps.Items {
		if err = s.syncConfigMapIntoNewNamespace(&configMap, ns); err != nil {
			return err
		}
	}

	secrets, err := s.kubeClient.CoreV1().Secrets(core.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, secret := range secrets.Items {
		if err = s.syncSecretIntoNewNamespace(&secret, ns); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConfigSyncer) syncerLabels(name, namespace, cluster string) labels.Set {
	return labels.Set{
		OriginNameLabelKey:      name,
		OriginNamespaceLabelKey: namespace,
		OriginClusterLabelKey:   cluster,
	}
}

func (s *ConfigSyncer) syncerLabelSelector(name, namespace, cluster string) string {
	return labels.SelectorFromSet(s.syncerLabels(name, namespace, cluster)).String()
}

func (s *ConfigSyncer) syncerAnnotations(oldAnnotations, srcAnnotations map[string]string, srcRef core.ObjectReference) map[string]string {
	newAnnotations := map[string]string{}

	// preserve sync annotations
	if v, ok := oldAnnotations[ConfigSyncKey]; ok {
		newAnnotations[ConfigSyncKey] = v
	}
	if v, ok := oldAnnotations[ConfigSyncContexts]; ok {
		newAnnotations[ConfigSyncContexts] = v
	}

	for k, v := range srcAnnotations {
		if k != ConfigSyncKey && k != ConfigSyncContexts {
			newAnnotations[k] = v
		}
	}

	// set origin reference
	ref, _ := json.Marshal(srcRef)
	newAnnotations[ConfigOriginKey] = string(ref)

	return newAnnotations
}
