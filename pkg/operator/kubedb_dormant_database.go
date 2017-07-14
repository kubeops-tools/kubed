package operator

import (
	"errors"
	"reflect"

	acrt "github.com/appscode/go/runtime"
	"github.com/appscode/kubed/pkg/util"
	"github.com/appscode/log"
	tapi "github.com/k8sdb/apimachinery/api"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	apiv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
)

// Blocks caller. Intended to be called as a Go routine.
func (op *Operator) WatchDormantDatabases() {
	if !util.IsPreferredAPIResource(op.KubeClient, tapi.V1alpha1SchemeGroupVersion.String(), tapi.ResourceKindDormantDatabase) {
		log.Warningf("Skipping watching non-preferred GroupVersion:%s Kind:%s", tapi.V1alpha1SchemeGroupVersion.String(), tapi.ResourceKindDormantDatabase)
		return
	}

	defer acrt.HandleCrash()

	lw := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return op.KubeDBClient.DormantDatabases(apiv1.NamespaceAll).List(metav1.ListOptions{})
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return op.KubeDBClient.DormantDatabases(apiv1.NamespaceAll).Watch(metav1.ListOptions{})
		},
	}
	_, ctrl := cache.NewInformer(lw,
		&tapi.DormantDatabase{},
		op.syncPeriod,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if res, ok := obj.(*tapi.DormantDatabase); ok {
					log.Infof("DormantDatabase %s@%s added", res.Name, res.Namespace)

					if op.Opt.EnableSearchIndex {
						if err := op.SearchIndex.HandleAdd(obj); err != nil {
							log.Errorln(err)
						}
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				if res, ok := obj.(*tapi.DormantDatabase); ok {
					log.Infof("DormantDatabase %s@%s deleted", res.Name, res.Namespace)
					if op.Opt.EnableSearchIndex {
						if err := op.SearchIndex.HandleDelete(obj); err != nil {
							log.Errorln(err)
						}
					}
					if op.TrashCan != nil {
						op.TrashCan.Delete(res.ObjectMeta, obj)
					}
				}
			},
			UpdateFunc: func(old, new interface{}) {
				oldRes, ok := old.(*tapi.DormantDatabase)
				if !ok {
					log.Errorln(errors.New("Invalid DormantDatabase object"))
					return
				}
				newRes, ok := new.(*tapi.DormantDatabase)
				if !ok {
					log.Errorln(errors.New("Invalid DormantDatabase object"))
					return
				}
				if op.Opt.EnableSearchIndex {
					op.SearchIndex.HandleUpdate(old, new)
				}
				if op.TrashCan != nil && op.Config.TrashCan.HandleUpdate {
					if !reflect.DeepEqual(oldRes.Labels, newRes.Labels) ||
						!reflect.DeepEqual(oldRes.Annotations, newRes.Annotations) ||
						!reflect.DeepEqual(oldRes.Spec, newRes.Spec) {
						op.TrashCan.Update(newRes.ObjectMeta, old, new)
					}
				}
			},
		},
	)
	ctrl.Run(wait.NeverStop)
}
