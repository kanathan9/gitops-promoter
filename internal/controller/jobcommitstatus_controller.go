/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"time"

	promoterv1alpha1 "github.com/argoproj-labs/gitops-promoter/api/v1alpha1"
	"github.com/argoproj-labs/gitops-promoter/internal/settings"
	"github.com/argoproj-labs/gitops-promoter/internal/types/constants"
	"github.com/argoproj-labs/gitops-promoter/internal/utils"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// JobCommitStatusReconciler reconciles a JobCommitStatus object.
//
// This reconciler currently only loads its parent resource and the referenced PromotionStrategy,
// and wires up the watches (PromotionStrategy via field index, owned Jobs) needed by later
// subtasks. It does not yet create child Jobs or manage CommitStatus resources; see issue #1597.
type JobCommitStatusReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	Recorder    events.EventRecorder
	SettingsMgr *settings.Manager

	// EnqueueCTP is a function to enqueue CTP reconcile requests without modifying the CTP object.
	// Unused until a later subtask starts transitioning CommitStatus phases, but plumbed through now
	// so every gate controller is constructed identically.
	EnqueueCTP CTPEnqueueFunc
}

// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses,verbs=get;list;watch
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=jobcommitstatuses/finalizers,verbs=update
// +kubebuilder:rbac:groups=promoter.argoproj.io,resources=promotionstrategies,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch

// Reconcile loads the JobCommitStatus and its referenced PromotionStrategy, and resolves the
// environments the gate applies to. Job creation, observation, and CommitStatus management are
// added in a later subtask; for now this only proves the watch/index/status plumbing works and
// that missing dependencies are handled without creating anything prematurely.
func (r *JobCommitStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling JobCommitStatus", "name", req.Name)
	startTime := time.Now()

	var jcs promoterv1alpha1.JobCommitStatus
	// This function applies the resource status via Server-Side Apply at the end of the reconciliation. Don't write status manually.
	var previousReady *metav1.Condition
	defer utils.HandleReconciliationResult(ctx, startTime, &jcs, r.Client, r.Recorder, constants.JobCommitStatusControllerFieldOwner, &result, &err, &previousReady)

	err = r.Get(ctx, req.NamespacedName, &jcs, &client.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			logger.Info("JobCommitStatus not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "failed to get JobCommitStatus")
		return ctrl.Result{}, fmt.Errorf("failed to get JobCommitStatus %q: %w", req.Name, err)
	}

	// Remove any existing Ready condition. We want to start fresh.
	previousReady = utils.RemoveReadyCondition(&jcs)

	if err := ensureControllerInstanceIDStable(ctx, r.SettingsMgr); err != nil {
		return ctrl.Result{}, err
	}

	// Fetch the referenced PromotionStrategy. HandleReconciliationResult records a Warning event
	// and sets Ready=False when this errors, so a missing reference is surfaced clearly without a
	// panic and without any child resource being created.
	var ps promoterv1alpha1.PromotionStrategy
	psKey := client.ObjectKey{
		Namespace: jcs.Namespace,
		Name:      jcs.Spec.PromotionStrategyRef.Name,
	}
	err = r.Get(ctx, psKey, &ps)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("referenced PromotionStrategy %q not found: %w", jcs.Spec.PromotionStrategyRef.Name, err)
		}
		return ctrl.Result{}, fmt.Errorf("failed to get PromotionStrategy %q: %w", jcs.Spec.PromotionStrategyRef.Name, err)
	}

	applicableEnvs := utils.GetApplicableEnvironments(&ps, jcs.Spec.Key, jcs.Spec.ReportOn)
	logger.Info("Resolved applicable environments for JobCommitStatus",
		"promotionStrategy", ps.Name,
		"key", jcs.Spec.Key,
		"reportOn", jcs.Spec.ReportOn,
		"environmentCount", len(applicableEnvs))

	// TODO(#1597): create/observe child Jobs per applicable environment and manage the
	// corresponding CommitStatus resources. Deliberately stubbed out for this subtask.

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
//
//nolint:dupl // Gate controllers share the same SetupWithManager skeleton by design.
func (r *JobCommitStatusReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	// Use Direct methods to read configuration from the API server without cache during setup.
	// The cache is not started during SetupWithManager, so we must use the non-cached API reader.
	rateLimiter, err := settings.GetRateLimiterDirect[promoterv1alpha1.JobCommitStatusConfiguration, ctrl.Request](ctx, r.SettingsMgr)
	if err != nil {
		return fmt.Errorf("failed to get JobCommitStatus rate limiter: %w", err)
	}

	maxConcurrentReconciles, err := settings.GetMaxConcurrentReconcilesDirect[promoterv1alpha1.JobCommitStatusConfiguration](ctx, r.SettingsMgr)
	if err != nil {
		return fmt.Errorf("failed to get JobCommitStatus max concurrent reconciles: %w", err)
	}

	err = ctrl.NewControllerManagedBy(mgr).
		For(&promoterv1alpha1.JobCommitStatus{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&promoterv1alpha1.PromotionStrategy{}, r.enqueueJobCommitStatusForPromotionStrategy()).
		Owns(&batchv1.Job{}).
		Named("jobcommitstatus").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrentReconciles,
			RateLimiter:             rateLimiter,
		}).
		Complete(r)
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}
	return nil
}

// enqueueJobCommitStatusForPromotionStrategy returns a handler that enqueues all JobCommitStatus
// resources that reference a PromotionStrategy when that PromotionStrategy changes. Lookups use
// PromotionStrategyRefField, the shared field index registered once for all gate CRDs by
// RegisterGatePromotionStrategyRefFieldIndexes (see fieldindex.go).
func (r *JobCommitStatusReconciler) enqueueJobCommitStatusForPromotionStrategy() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []ctrl.Request {
		ps, ok := obj.(*promoterv1alpha1.PromotionStrategy)
		if !ok {
			return nil
		}

		var jcsList promoterv1alpha1.JobCommitStatusList
		if err := r.List(ctx, &jcsList,
			client.InNamespace(ps.Namespace),
			client.MatchingFields{PromotionStrategyRefField: ps.Name},
		); err != nil {
			log.FromContext(ctx).Error(err, "failed to list JobCommitStatus resources")
			return nil
		}

		requests := make([]ctrl.Request, 0, len(jcsList.Items))
		for i := range jcsList.Items {
			requests = append(requests, ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(&jcsList.Items[i]),
			})
		}

		return requests
	})
}
