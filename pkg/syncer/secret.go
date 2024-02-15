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
	context "context"

	"kubeops.dev/config-syncer/pkg/eventer"

	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	core_util "kmodules.xyz/client-go/core/v1"
)

func (s *ConfigSyncer) SyncSecret(src *core.Secret) error {
	opts := GetSyncOptions(src.Annotations)

	if opts.NamespaceSelector != nil { // delete that were in old-ns but not in new-ns and upsert to new-ns
		newNs, err := NamespacesForSelector(s.kubeClient, *opts.NamespaceSelector)
		if err != nil {
			return err
		}
		klog.Infof("secret %s/%s will be synced into namespaces %v if needed", src.Namespace, src.Name, newNs.List())
		if err := s.syncSecretIntoNamespaces(s.kubeClient, src, newNs, true, ""); err != nil {
			return err
		}
	} else { // no sync, delete that were previously added
		if err := s.syncSecretIntoNamespaces(s.kubeClient, src, sets.NewString(), true, ""); err != nil {
			return err
		}
	}

	return s.syncSecretIntoContexts(src, opts.Contexts)
}

// source deleted, delete that were previously added
func (s *ConfigSyncer) SyncDeletedSecret(src *core.Secret) error {
	if err := s.syncSecretIntoNamespaces(s.kubeClient, src, sets.NewString(), true, ""); err != nil {
		return err
	}
	return s.syncSecretIntoContexts(src, sets.NewString())
}

func (s *ConfigSyncer) syncSecretIntoContexts(src *core.Secret, contexts sets.String) error {
	// validate contexts specified via annotation
	taken := map[string]struct{}{}
	for _, ctx := range contexts.List() {
		context, found := s.contexts[ctx]
		if !found {
			return errors.Errorf("context %s not found in kubeconfig file", ctx)
		}
		if _, found = taken[context.Address]; found {
			return errors.Errorf("multiple contexts poniting same cluster")
		}
		taken[context.Address] = struct{}{}
	}

	// sync to contexts specified via annotation, do not ignore errors here
	for _, ctx := range contexts.List() {
		context := s.contexts[ctx]
		if context.Namespace == "" { // use source namespace if not specified via context
			context.Namespace = src.Namespace
		}
		err := s.syncSecretIntoNamespaces(context.Client, src, sets.NewString(context.Namespace), false, ctx)
		if err != nil {
			return err
		}
	}

	// delete from other contexts, ignore errors here
	for ctxName, ctx := range s.contexts {
		if _, found := taken[ctx.Address]; !found {
			err := s.syncSecretIntoNamespaces(ctx.Client, src, sets.NewString(), false, ctxName)
			if err != nil {
				klog.Infoln(err)
			}
			taken[ctx.Address] = struct{}{} // to avoid deleting form same cluster twice
		}
	}

	return nil
}

// upsert into newNs set, delete from (oldNs-newNs) set
// use skipSrcNs = true for sync in source cluster
func (s *ConfigSyncer) syncSecretIntoNamespaces(kc kubernetes.Interface, src *core.Secret, newNs sets.String, skipSrcNs bool, ctx string) error {
	oldNs, err := namespaceSetForSecretSelector(kc, s.syncerLabelSelector(src.Name, src.Namespace, s.clusterName))
	if err != nil {
		return err
	}
	oldNs = oldNs.Difference(newNs)
	if skipSrcNs {
		oldNs.Delete(src.Namespace)
		newNs.Delete(src.Namespace)
	}
	for _, ns := range oldNs.List() {
		if err := kc.CoreV1().Secrets(ns).Delete(context.TODO(), src.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}
	for _, ns := range newNs.List() {
		if err = s.upsertSecret(kc, src, ns, ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *ConfigSyncer) syncSecretIntoNewNamespace(src *core.Secret, namespace *core.Namespace) error {
	opts := GetSyncOptions(src.Annotations)
	if opts.NamespaceSelector == nil {
		return nil
	}
	if selector, err := labels.Parse(*opts.NamespaceSelector); err != nil {
		return err
	} else if selector.Matches(labels.Set(namespace.Labels)) {
		return s.upsertSecret(s.kubeClient, src, namespace.Name, "")
	}
	return nil
}

func (s *ConfigSyncer) upsertSecret(kc kubernetes.Interface, src *core.Secret, namespace, ctx string) error {
	meta := metav1.ObjectMeta{
		Name:      src.Name,
		Namespace: namespace,
	}
	_, _, err := core_util.CreateOrPatchSecret(context.TODO(), kc, meta, func(obj *core.Secret) *core.Secret {
		// check origin cluster, if not match overwrite and create an event
		if v, ok := obj.Labels[OriginClusterLabelKey]; ok && v != s.clusterName {
			s.recorder.Eventf(
				src,
				core.EventTypeWarning,
				eventer.EventReasonOriginConflict,
				"Origin cluster changed from %s in context %s", v, ctx,
			)
		}

		obj.Type = src.Type
		obj.Data = src.Data
		obj.Labels = labels.Merge(src.Labels, s.syncerLabels(src.Name, src.Namespace, s.clusterName))
		obj.Kind = src.Kind

		ref := core.ObjectReference{
			APIVersion:      src.APIVersion,
			Kind:            src.Kind,
			Name:            src.Name,
			Namespace:       src.Namespace,
			UID:             src.UID,
			ResourceVersion: src.ResourceVersion,
		}
		obj.Annotations = s.syncerAnnotations(obj.Annotations, src.Annotations, ref)
		s.applyFinalizers(obj.Labels, obj.Annotations, src.Annotations)

		return obj
	}, metav1.PatchOptions{})

	return err
}

func namespaceSetForSecretSelector(kc kubernetes.Interface, selector string) (sets.String, error) {
	secret, err := kc.CoreV1().Secrets(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, err
	}
	ns := sets.NewString()
	for _, obj := range secret.Items {
		ns.Insert(obj.Namespace)
	}
	return ns, nil
}
